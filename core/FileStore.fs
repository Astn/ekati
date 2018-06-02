namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc


type GrpcFileStore(config:Config) = 

    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<NodeIdHash, seq<Grpc.MemoryPointer>>()
    

    let IndexMaintainer =
        MailboxProcessor<IndexMessage>.Start(fun inbox ->
            let rec messageLoop() = 
                async{
                    let! message = inbox.Receive()
                    match message with 
                    | Index(nid) ->
                            let id = nid
                            let seqId = [id.Pointer] |> Seq.ofList
                            ``Index of NodeID -> MemoryPointer``.AddOrUpdate(Utils.GetNodeIdHash id, [nid.Pointer], 
                                (fun x y -> 
                                    let appended = y |> Seq.append seqId
                                    appended
                                    )) |> ignore
                    | Flush(replyChannel)->
                        replyChannel.Reply(true)
                        
                    return! messageLoop()
                }    
            messageLoop()    
        )


    let PartitionWriters = 
        let bcs = 
            seq {for i in 0 .. (config.ParitionCount - 1) do 
                 yield i}
            |> Array.ofSeq
        bcs    
        |>  Seq.map (fun (i) -> 
                
            let partition = FileStorePartition(config,i,ClusterServices(``Index of NodeID -> MemoryPointer``, IndexMaintainer))   
            
            (partition.IORequests(), partition.Thread())
            )            
        |> Array.ofSeq                 

    
    let setTimestamps (node:Node) (nowInt:Int64) =
        for kv in node.Attributes do
            kv.Key.Timestamp <- nowInt
            for v in kv.Value do
            v.Timestamp <- nowInt
    
    let Flush () =
        let parentTask = Task.Factory.StartNew((fun () ->
            let allDone =
                seq {for (bc,t) in PartitionWriters do
                        let fwtcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)
                        bc.Add( FlushWrites(fwtcs))
                        let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)
                        bc.Add( FlushFixPointers(tcs))
                        let ffltcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)
                        bc.Add( FlushFragmentLinks(ffltcs))
                        yield [ fwtcs.Task :> Task; tcs.Task :> Task; ffltcs.Task :> Task]}
                |> Seq.collect (fun x -> x)
                |> List.ofSeq // force it to run
            allDone    
            ))
        parentTask.Wait()        
        ()
                    
    interface IStorage with
        member x.Nodes = 
            // return local nodes before remote nodes
            // let just start by pulling nodes from the index.
            
            // todo: this could be a lot smarter and fetch from more than one partition reader at a time
            // todo: additionally, Using the index likely results in Random file access, we could instead
            // todo: just read from the file sequentially
            seq { for kv in ``Index of NodeID -> MemoryPointer`` do
                  for fragment in kv.Value do
                      let tcs = new TaskCompletionSource<Node>()
                      let (bc,t) = PartitionWriters.[int <| fragment.Partitionkey]
                      bc.Add (Read( tcs, fragment)) 
                      yield tcs.Task.Result
                }
            // todo return remote nodes
            
        member x.Flush () = Flush()
            
        member this.Add (nodes:seq<Node>) = 
            Task.Factory.StartNew(fun () -> 
                // TODO: Might need to have multiple add functions so the caller can specify a time for the operation
                // Add time here so it's the same for all TMDs
                let nowInt = DateTime.UtcNow.ToBinary()
                let timer = Stopwatch.StartNew()
                let partitionLists = 
                    seq {for i in 0 .. (config.ParitionCount - 1) do 
                         yield new System.Collections.Generic.List<Node>()}
                    |> Array.ofSeq
                    
                let mutable i = 0    
                for node in nodes do 
                    setTimestamps node nowInt
                    partitionLists.[i % config.ParitionCount].Add node
                    i <- i + 1
                    
                timer.Stop()
                let timer2 = Stopwatch.StartNew()
                partitionLists
                    |> Seq.iteri (fun i list ->
                        if (list.Count > 0) then
                            let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)         
                            let (bc,t) = PartitionWriters.[i]
                            bc.Add (Write(tcs,list))
                        )
                timer2.Stop()
                config.log(sprintf "grouping: %A tasking: %A" timer.Elapsed timer2.Elapsed)
                )
                
                        
        member x.Remove (nodes:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<AddressBlock>) = 
            let requestsMade =
                addressBlock
                |> Seq.map (fun ab ->
                    let tcs = new TaskCompletionSource<Node>()
                    let nodeId = 
                        match ab.AddressCase with 
                        | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid
                        | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid
                        | _ -> raise (new NotImplementedException("AddressBlock did not contain a valid NodeID"))
                    
                    let (bc,t) = PartitionWriters.[int <| nodeId.Pointer.Partitionkey]
                    let nid = match ab.AddressCase with 
                              | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid
                              | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid
                              | _ -> raise (new ArgumentException("AddressBlock did not contain a valid AddressCase"))
                    
                    // TODO: Read all the fragments, not just the first one.
                    let t = 
                        if (nid.Pointer = Utils.NullMemoryPointer()) then
                            let mutable mp = Seq.empty<MemoryPointer>
                            if(``Index of NodeID -> MemoryPointer``.TryGetValue(Utils.GetNodeIdHash nid, &mp)) then 
                                bc.Add (Read(tcs, mp |> Seq.head))
                                tcs.Task
                            else 
                                tcs.SetException(new KeyNotFoundException("Index of NodeID -> MemoryPointer: did not contain the NodeID")) 
                                tcs.Task   
                        else 
                            bc.Add(Read(tcs,nid.Pointer))
                            tcs.Task
                            
                    let res = t.ContinueWith(fun (isdone:Task<Node>) ->
                                if (isdone.IsCompletedSuccessfully) then
                                    ab,Left(isdone.Result)
                                else 
                                    ab,Right(isdone.Exception :> Exception)
                                )
                                
                    
                    res)
            
            Task.FromResult 
                (seq { for t in requestsMade do 
                       yield t.Result 
                })  
        member x.First (predicate: (Node -> bool)) = raise (new NotImplementedException())
        member x.Stop () =  
            Flush()
            for (bc,t) in PartitionWriters do
                bc.CompleteAdding()
                t.Join()    
            while IndexMaintainer.CurrentQueueLength > 0 do 
                Thread.Sleep(10)            
