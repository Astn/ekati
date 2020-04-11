namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data.SqlTypes
open System.Diagnostics
open System.IO
open System.Linq
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc

type ClusterServices() = 
    let remotePartitions = new ConcurrentDictionary<int,FileStorePartition>()
    member this.RemotePartitions() = remotePartitions
    interface IClusterServices with 
        member this.RemoteLookup (partition:int) (hash:NodeIdHash) : bool * MemoryPointer = 
            if remotePartitions.ContainsKey partition then 
                let remote = remotePartitions.[ partition ]
                let mutable refPointers :Pointers = null
                let rind = remote.Index()
                if rind.TryGetValue(hash, & refPointers) then
                    true, refPointers.Pointers_ |> Seq.head
                else
                    false, Utils.NullMemoryPointer()
                
            else false, Utils.NullMemoryPointer()    
            


type GrpcFileStore(config:Config) = 

    let clusterServices = new ClusterServices()

    let PartitionWriters = 
        let bcs = 
            seq {for i in 0 .. (config.ParitionCount - 1) do 
                 yield i}
            |> Array.ofSeq
        let writers = 
            bcs    
            |>  Seq.map (fun (i) -> 
                    
                let partition = FileStorePartition(config,i,clusterServices)   
                
                (partition.IORequests(), partition.Thread(), partition)
                )            
            |> Array.ofSeq
        
        for i in 0 .. (writers.Length - 1) do
            let (_,_,part) = writers.[i]
            clusterServices.RemotePartitions().AddOrUpdate(i,part, (fun x p -> part)) |> ignore
            
            
        writers                     

    
    let setTimestamps (node:Node) (nowInt:Int64) =
        for kv in node.Attributes do
            kv.Key.Timestamp <- nowInt
            kv.Value.Timestamp <- nowInt
    
    let Flush () =
        let parentTask = Task.Factory.StartNew((fun () ->
            let allDone =
                seq {for (bc,t,_) in PartitionWriters do
                        let fwtcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)
                        while bc.Writer.TryWrite ( FlushAdds(fwtcs)) = false do ()
                        let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)
                        while bc.Writer.TryWrite ( FlushFixPointers(tcs)) = false do ()
                        let ffltcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)
                        while bc.Writer.TryWrite ( FlushFragmentLinks(ffltcs)) = false do ()
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
            let numWriters = PartitionWriters.Length
            let scanners = 
                seq {
                    for bc,t,part in PartitionWriters do
                        yield part.ScanIndex()
                } |> Array.ofSeq
            let currentChunks = 
                scanners 
                |> Array.map (fun scanner -> scanner.First)

            let currentPositions = Array.zeroCreate<int> numWriters 
                

            // todo: We could improve throughput by quite a bit if we requested batches of contigious nodes
            let requests () = 
                seq { 
                  for i in 0 .. numWriters - 1 do
                    if currentChunks.[i] <> null && currentPositions.[i] = currentChunks.[i].Value.Count then
                        // reset currentPosition
                        currentPositions.[i] <- 0
                        // move CurrentChunk to next chunk
                        currentChunks.[i] <- currentChunks.[i].Next   
                    
                    // if we still have data.    
                    if currentChunks.[i] <> null && currentPositions.[i] < currentChunks.[i].Value.Count then 
                        yield  i,  currentChunks.[i].Value.Item(currentPositions.[i])
                        
                    currentPositions.[i] <- currentPositions.[i] + 1    
                } 
             
            seq {
                let mutable finished = false
                while not finished do
                    let req = 
                        seq { for i in 0 .. 256 do yield requests() }
                        |> Seq.collect (fun x -> x ) 
                        |> Seq.map (fun x -> x )
                        |> Seq.groupBy ( fun (i,x) -> i)
                        |> Seq.map(fun (i,xs) -> 
                            let tcs = new TaskCompletionSource<Node[]>()
                            let bc,_,_ = PartitionWriters.[i]
                            let written = bc.Writer.WriteAsync(Read(tcs,xs |> Seq.map(fun (i,x)->x) |> Array.ofSeq))
                            written, tcs.Task)
                        |> Array.ofSeq
                            
                    if req.Length = 0 then
                        finished <- true
                    else
                        for (written, result) in req do
                            if written.IsCompletedSuccessfully then 
                                yield result.Result
                            else 
                                written.AsTask().Wait()
                                yield result.Result    
                
            }
            |> Seq.collect(fun x->x)
            // todo return remote nodes
            
        member x.Flush () = Flush()
            
        member this.Add (nodes:seq<Node>) = 
            Task.Factory.StartNew(fun () -> 
                use addTimer = config.Metrics.Measure.Timer.Time(Metrics.FileStoreMetrics.AddTimer)
                // TODO: Might need to have multiple add functions so the caller can specify a time for the operation
                // Add time here so it's the same for all TMDs
                let nowInt = DateTime.UtcNow.ToBinary()
                
                let partitionLists = 
                    seq {for i in 0 .. (config.ParitionCount - 1) do 
                         yield new System.Collections.Generic.List<Node>()}
                    |> Array.ofSeq
                
                
                let lstNodes = nodes |> List.ofSeq
                let count = int64 lstNodes.Length  

                Parallel.For(0,lstNodes.Length,(fun i ->
                    let node = lstNodes.[i]
                    setTimestamps node nowInt
                    let nodeHash = Utils.GetAddressBlockHash node.Id
                    let partition = Utils.GetPartitionFromHash config.ParitionCount nodeHash
                    let plist = partitionLists.[partition] 
                    lock (plist) (fun () -> plist.Add node) 
                )) |> ignore
               
                partitionLists
                    |> Seq.iteri (fun i list ->
                        if (list.Count > 0) then
                            let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)         
                            let (bc,_,_) = PartitionWriters.[i]
                            while bc.Writer.TryWrite ( Add(tcs,list)) = false do ()
                        )
                
                config.Metrics.Measure.Meter.Mark(Metrics.FileStoreMetrics.AddFragmentMeter, count)
                
                )
                
                        
        member x.Remove (nodes:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<AddressBlock>) = 
            let requestsMade =
                addressBlock
                |> Seq.map (fun ab ->
                    
                    let tcs = new TaskCompletionSource<Node[]>()
                    let nid = 
                        match ab.AddressCase with 
                        | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid
                        | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid
                        | _ -> raise (new NotImplementedException("AddressBlock did not contain a valid NodeID"))
                    assert (nid.Pointer <> Utils.NullMemoryPointer())
                    let (bc,t,part) = PartitionWriters.[int <| nid.Pointer.Partitionkey]
                    
                    // TODO: Read all the fragments, not just the first one.
                    let t = 
                        if (nid.Pointer = Utils.NullMemoryPointer()) then
                            let mutable mp:Pointers = null
                            if(part.Index().TryGetValue(Utils.GetNodeIdHash nid, &mp)) then 
                                while bc.Writer.TryWrite (Read(tcs, mp.Pointers_ |> Seq.take 1 |> Array.ofSeq)) = false do ()
                                tcs.Task
                            else 
                                tcs.SetException(new KeyNotFoundException("Index of NodeID -> MemoryPointer: did not contain the NodeID")) 
                                tcs.Task   
                        else 
                            while bc.Writer.TryWrite (Read(tcs, [|nid.Pointer|])) = false do ()
                            tcs.Task
                            
                    let res = t.ContinueWith(fun (isdone:Task<Node[]>) ->
                                if (isdone.IsCompletedSuccessfully) then
                                    config.Metrics.Measure.Meter.Mark(Metrics.FileStoreMetrics.ItemFragmentMeter)
                                    ab,Left(isdone.Result)
                                else 
                                    ab,Right(isdone.Exception :> Exception)
                                )
                    res)
            
            Task.FromResult 
                (seq { use itemTimer = config.Metrics.Measure.Timer.Time(Metrics.FileStoreMetrics.ItemTimer)
                       for ts in requestsMade do
                        let (ab,eith) = ts.Result
                        yield 
                            match eith with 
                            | Left(nodes) -> (ab,Left(nodes.[0])) // TODO: Fix: Currently ignoring everything after first result.
                            | Right(err) -> (ab,Right(err))
                })  
        member x.First (predicate: (Node -> bool)) = raise (new NotImplementedException())
        member x.Stop () =  
            Flush()
            for (bc,t,part) in PartitionWriters do
                bc.Writer.Complete()
                t.Join() // wait for shard threads to stop    
