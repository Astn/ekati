namespace Ekati


open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data.SqlTypes
open System.Diagnostics
open System.IO
open System.Linq
open System.Net.NetworkInformation
open System.Threading
open System.Threading
open System.Threading.Tasks
open FlatBuffers
open Mono.Unix.Native
open Ekati.Core
open Ekati.Ext
type ClusterServices(log: string -> unit) = 
    let remotePartitions = new ConcurrentDictionary<int,FileStorePartition>()
    
    let ByteToHex (bytes:byte[]) =
        bytes |> Array.fold (fun state x-> state + sprintf "%02X" x) ""
    member this.RemotePartitions() = remotePartitions
    interface IClusterServices with 
        member this.RemoteLookup (partition:int) (nid:NodeID) : bool * Node = 
            if remotePartitions.ContainsKey partition then 
                let remote = remotePartitions.[ partition ]
                let mutable refPointers : Node = Node()
                let rind = remote.AttrIndex()
                if rind.TryGetValue(nid, & refPointers) then
                    true, refPointers
                else
                    false, Node()
                
            else false, Node()
            


type GrpcFileStore(config:Config) = 

    let clusterServices = new ClusterServices(config.log)
    let writers bcs = 
        bcs    
        |>  Seq.map (fun (i) -> 
                
            let partition = FileStorePartition(config,i,clusterServices)   
            
            (partition.IORequests(), partition.Thread(), partition)
            )            
        |> Array.ofSeq
    let PartitionWriters = 
        let bcs = 
            seq {for i in 0 .. (config.ParitionCount - 1) do 
                 yield i}
            |> Array.ofSeq
            
        let writers = writers bcs
        
        for i in 0 .. (writers.Length - 1) do
            let (_,_,part) = writers.[i]
            clusterServices.RemotePartitions().AddOrUpdate(i,part, (fun x p -> part)) |> ignore
        writers
   
    let Flush () =
        ()       
            
    let StartQueryNodeId (nid:NodeID) : Async<Node> =
        let partition = Utils.GetPartitionFromHash(config.ParitionCount, nid)
        let (bc,t,part) = PartitionWriters.[int <| partition]
        async {
            let! stuff = part.AttrIndex().TryGetValueAsync(nid).AsTask() |> Async.AwaitTask
            let struct (status,node) = stuff.CompleteRead()
            config.Metrics.Measure.Meter.Mark(Metrics.FileStoreMetrics.ItemFragmentMeter)
            return
                match status with
                | FASTER.core.Status.OK -> node
                | FASTER.core.Status.ERROR -> raise <| Exception("ERROR")
                | FASTER.core.Status.PENDING -> raise <| Exception("PENDING")
                | FASTER.core.Status.NOTFOUND -> raise <| KeyNotFoundException("Index of NodeID -> MemoryPointer: did not contain the NodeID " + nid.Iri)
        }
    
    let StartScan (): seq<Node> =
        seq {
                for bc,t,part in PartitionWriters do
                    yield part.AttrIndex().Iter()
            } 
        |> Seq.collect(fun x -> x)  
        
    let LoadNode(addressBlock:seq<NodeID>, filter:BloomFilter.Filter<int>) : seq<Node> =
        addressBlock
        |> Seq.distinctBy (fun x -> x.Iri)
        |> Seq.filter (fun ab ->
            if ab.Iri = "*" then // match anything
                true
            else
                let hash = ab.Iri.GetHashCode()
                let loadIt = filter.Contains(hash) |> not
                filter.Add(hash) |> ignore
                loadIt
            )
        |> Seq.collect (fun nid ->
            if nid.Iri = "*" then
                StartScan () 
            else
                [ StartQueryNodeId nid ] |> Seq.map ( fun future -> Async.RunSynchronously future ) 
            )
    
    let rec QueryNodes(addressBlock:seq<NodeID>, filter:BloomFilter.Filter<int>) : seq<Node> =        
        LoadNode(addressBlock, filter)
        |> Seq.truncate 1000
                    
    let QueryNoDuplicates(addressBlock:seq<NodeID>) : System.Threading.Tasks.Task<seq<Node>> =
        let filter = BloomFilter.FilterBuilder.Build<int>(10000)
        let nodeStream = QueryNodes(addressBlock, filter)
        nodeStream
            |> Task.FromResult
    
    let Stop() =
        Flush()
        for (bc,t,part) in PartitionWriters do
            bc.Writer.Complete()
            t.Join() // wait for shard threads to stop    
    let proc = Process.GetCurrentProcess() 
    let collectSystemMetrics() =
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.HandleCountGauge, float proc.HandleCount)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.NonPagedSystemMemorySizeGauge, float proc.NonpagedSystemMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PagedSystemMemorySizeGauge, float proc.PagedSystemMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PagedMemorySizeGauge, float proc.PagedMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PeakPagedMemorySizeGauge, float proc.PeakPagedMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PrivateMemorySizeGauge, float proc.PrivateMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.VirtualMemorySizeGauge, float proc.VirtualMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PeakVirtualMemorySizeGauge, float proc.PeakVirtualMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PeakWorkingSetGauge, float proc.PeakWorkingSet64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.WorkingSetGauge, float proc.WorkingSet64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.TotalProcessorTimeGauge, float proc.TotalProcessorTime.TotalMilliseconds)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PrivilegedProcessorTimeGauge, float proc.PrivilegedProcessorTime.TotalMilliseconds)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.UserProcessorTimeGauge, float proc.UserProcessorTime.TotalMilliseconds)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcEstimatedMemorySizeGauge, float (GC.GetTotalMemory(false)))
            let maxGen = GC.MaxGeneration
            if (0 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount0Gauge, float (GC.CollectionCount(0)))
            if (1 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount1Gauge, float (GC.CollectionCount(1)))
            if (2 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount2Gauge, float (GC.CollectionCount(2)))
            if (3 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount3Gauge, float (GC.CollectionCount(3)))
            if (4 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount4Gauge, float (GC.CollectionCount(4)))        

            ()    
    
//    let GetStats(req:GetStatsRequest, cancel: CancellationToken): Task<GetStatsResponse> =
//        
//        Task.FromResult(GetStatsResponse())
//    let GetMetrics(req:GetMetricsRequest, cancel: CancellationToken): Task<GetMetricsResponse> =
//        collectSystemMetrics()
//        let snap = config.Metrics.Snapshot
//        let x = snap.Get()
//        let ts = x.Timestamp
//        let gmr = GetMetricsResponse()
//        for context in x.Contexts do
//            for counter in context.Counters do
//                let met = GetMetricsResponse.Types.Metric()
//                met.Name <- counter.Name
//                met.Time <- Timestamp.FromDateTime ts
//                met.Value <- float32 counter.Value.Count
//                gmr.Metrics.Add (met)
//            for guage in context.Gauges do
//                let met = GetMetricsResponse.Types.Metric()
//                met.Name <- guage.Name
//                met.Time <- Timestamp.FromDateTime ts
//                met.Value <- float32 guage.Value
//                gmr.Metrics.Add (met)
//            for meter in context.Meters do
//                let met = GetMetricsResponse.Types.Metric()
//                met.Name <- meter.Name
//                met.Time <- Timestamp.FromDateTime ts
//                met.Value <- float32 meter.Value.OneMinuteRate
//                gmr.Metrics.Add (met)
//            for hist in context.Histograms do
//                let met = GetMetricsResponse.Types.Metric()
//                met.Name <- hist.Name
//                met.Time <- Timestamp.FromDateTime ts
//                met.Value <- float32 hist.Value.Count
//                gmr.Metrics.Add (met)
//            for t in context.Timers do
//                let met = GetMetricsResponse.Types.Metric()
//                met.Name <- t.Name
//                met.Time <- Timestamp.FromDateTime ts
//                met.Value <- float32 t.Value.Rate.OneMinuteRate
//                gmr.Metrics.Add (met)
//                () 
//        Task.FromResult(gmr)
    
    interface IDisposable with
        member x.Dispose() = Stop()
        
    interface IStorage with
    
//        member x.GetStats(req, cancel) =
//            GetStats(req, cancel)
//        
//        member x.GetMetrics(req, cancel) =
//            GetMetrics(req,cancel)
        
        member x.Nodes() : IEnumerable<Node> = 
            StartScan()
            
        member x.Flush () = Flush()
            
        member this.Add (nodes:seq<Node>) =
            nodes
                // can we partition by instead of group?
                |> Seq.groupBy(fun n ->
                    let mutable nid = n.Id.Value
                    let i = Utils.GetPartitionFromHash(PartitionWriters.Length, nid  )
                    i)
                |> Seq.map (fun (group, items) ->
                    let tcs = TaskCompletionSource<unit>()         
                    let (bc,_,_) = PartitionWriters.[group]
                    while bc.Writer.TryWrite ( Adds(tcs, items)) = false do
                        Console.WriteLine "Couldn't Add"
                    Console.WriteLine "Sent node to channel"
                    tcs.Task :> Task
                    )
                |> Seq.toArray
                |> Task.WhenAll
                        
        member x.Remove (nodes:seq<NodeID>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<NodeID>) = QueryNoDuplicates(addressBlock)
        member x.First (predicate: Func<Node, bool>) = raise (new NotImplementedException())
        member x.Stop () =  
            Flush()
            for (bc,t,part) in PartitionWriters do
                bc.Writer.Complete()
                t.Join() // wait for shard threads to stop    
