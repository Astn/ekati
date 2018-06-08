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
                let mutable refPointers = Seq.empty<MemoryPointer>
                if remote.Index().TryGetValue(hash, & refPointers) then
                    true, refPointers |> Seq.head
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
            for v in kv.Value do
            v.Timestamp <- nowInt
    
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
            
            // todo: this could be a lot smarter and fetch from more than one partition reader at a time
            // todo: additionally, Using the index likely results in Random file access, we could instead
            // todo: just read from the file sequentially
            seq { for bc,t,part in PartitionWriters do
                  for kv in part.Index() do
                  for fragment in kv.Value do
                      let tcs = new TaskCompletionSource<Node>()
                      while bc.Writer.TryWrite ( Read( tcs, fragment)) = false do ()
                      yield tcs.Task.Result
                }
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
                let withpartition = Array.zeroCreate<int * Node> lstNodes.Length
                Parallel.For(0,lstNodes.Length,(fun i ->
                    let node = lstNodes.[i]
                    setTimestamps node nowInt
                    let nodeHash = Utils.GetAddressBlockHash node.Id
                    let partition = Utils.GetPartitionFromHash config.ParitionCount nodeHash
                    withpartition.[i] <- partition, node
                )) |> ignore
                
                for i, node in withpartition do 
                    partitionLists.[i].Add node                   
                
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
                    let tcs = new TaskCompletionSource<Node>()
                    let nid = 
                        match ab.AddressCase with 
                        | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid
                        | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid
                        | _ -> raise (new NotImplementedException("AddressBlock did not contain a valid NodeID"))
                    
                    let (bc,t,part) = PartitionWriters.[int <| nid.Pointer.Partitionkey]
                    
                    // TODO: Read all the fragments, not just the first one.
                    let t = 
                        if (nid.Pointer = Utils.NullMemoryPointer()) then
                            let mutable mp = Seq.empty<MemoryPointer>
                            if(part.Index().TryGetValue(Utils.GetNodeIdHash nid, &mp)) then 
                                while bc.Writer.TryWrite (Read(tcs, mp |> Seq.head)) = false do ()
                                tcs.Task
                            else 
                                tcs.SetException(new KeyNotFoundException("Index of NodeID -> MemoryPointer: did not contain the NodeID")) 
                                tcs.Task   
                        else 
                            while bc.Writer.TryWrite (Read(tcs,nid.Pointer)) = false do ()
                            tcs.Task
                            
                    let res = t.ContinueWith(fun (isdone:Task<Node>) ->
                                if (isdone.IsCompletedSuccessfully) then
                                    config.Metrics.Measure.Meter.Mark(Metrics.FileStoreMetrics.ItemFragmentMeter)
                                    ab,Left(isdone.Result)
                                else 
                                    ab,Right(isdone.Exception :> Exception)
                                )
                                
                    
                    res)
            
            Task.FromResult 
                (seq { use itemTimer = config.Metrics.Measure.Timer.Time(Metrics.FileStoreMetrics.ItemTimer)
                       for t in requestsMade do 
                       yield t.Result 
                })  
        member x.First (predicate: (Node -> bool)) = raise (new NotImplementedException())
        member x.Stop () =  
            Flush()
            for (bc,t,part) in PartitionWriters do
                bc.Writer.Complete()
                t.Join() // wait for shard threads to stop    
