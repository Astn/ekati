namespace Ekati

open Ekati.Core
open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Linq
open System.Threading
open System.Threading.Tasks
open App.Metrics
open Metrics
open Microsoft.AspNetCore.Routing.Tree

type FileStorePartition(config:Config, i:int, cluster:IClusterServices) = 
    let tags = MetricTags([| "partition_id" |],
                          [| i.ToString() |]) 

    let dir = match config.CreateTestingDataDirectory with 
                      | true -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("data-"+ Path.GetRandomFileName())))
                      | false -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data"))

    let bc = System.Threading.Channels.Channel.CreateBounded<NodeIO>(1000)
        
    // TODO: Are these per file, with bloom filters, or aross files in the same shard?
    let ``Index of NodeID -> Attributes`` = new NodeStore(Path.Combine(dir.FullName, if config.CreateTestingDataDirectory then Path.GetRandomFileName() else "nodeattrs"+i.ToString()))
    let DoMaintenance() =
        NoOP()
    let IOThread =  
        let t = new ThreadStart((fun () -> 
            try
                let reader = bc.Reader
                let alldone = reader.Completion
                let myNoOp = NoOP()
                let mutable count = 0L
                let mutable readCount = 0L
                while alldone.IsCompleted = false do
                    let mutable nio: NodeIO = NodeIO.NoOP() 
                    if reader.TryRead(&nio) = false then
                        let nioWaitTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.QueueEmptyWaitTime, tags)
                        // sleep for now.
                        // todo: use this down time to do cleanup and compaction and other maintenance tasks.
                        
                        let nioTask = DoMaintenance()
                        if nioTask <> NoOP() then
                            nio <- nioTask
                        else    
                            reader.WaitToReadAsync().AsTask()
                                |> Async.AwaitTask
                                |> Async.RunSynchronously
                                |> ignore
                            
                            nio <- myNoOp // set NoOp so we can loop and check alldone.IsCompleted again.
                        nioWaitTimer.Dispose()
                        
                        
                    match nio with
                    | Adds(tcs,nodes) ->
                        try
                            let writeTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.AddTimer, tags)
                            for node in nodes do
                                count <- count + ``Index of NodeID -> Attributes``.AddOrUpdateBatch(node)
                            
                            writeTimer.Dispose()
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddFragmentMeter, tags, count)
                            tcs.SetResult()
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | Add(tcs,node) -> 
                        try
                            let writeTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.AddTimer, tags)
                           
                            count <- count + ``Index of NodeID -> Attributes``.AddOrUpdateBatch(node)
                            
                            writeTimer.Dispose()
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddFragmentMeter, tags, count)
                            tcs.SetResult()
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | Read (tcs,requests) ->  
                        try 
                            let ReadTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.ReadTimer, tags)

                            if requests.Length > 0 then
                                let outNodes = List<Node>(requests.Length)
                                for r in requests do
                                    let mutable node: Node = Node()
                                    if ``Index of NodeID -> Attributes``.TryGetValue(r, &node) then
                                        outNodes.Add(node)
                                readCount <- readCount + (Convert.ToInt64 requests.Length)
                                config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.ReadNodeFragmentCount, tags, int64 readCount)
                                tcs.SetResult(outNodes.ToArray())
                                
                            else
                                tcs.SetResult(array.Empty<Node>())   
                            ReadTimer.Dispose()
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | FlushFixPointers (tcs, ptrs) ->
                            tcs.SetResult()  
                    | NoOP(u) -> u
                                                                      
            finally
                config.log <| sprintf "Shutting down partition writer[%A]" i 
                (``Index of NodeID -> Attributes`` :> IDisposable).Dispose()
                config.log <| sprintf "Shutting down partition writer[%A] :: Success" i                     
            ()))
        
        let thread = new Thread(t)
        thread.Start()          
        thread        
        
    // TODO: don't expose this    
    member __.Thread() = IOThread
    // TODO: don't directly expose the blocking collection. Replace with functions they can use, that way we can replace the implementation without affecting the caller
    member __.IORequests() = bc  
    // NOTE: This is allowing access to our index by other threads
    member __.AttrIndex() = ``Index of NodeID -> Attributes``
