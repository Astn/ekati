namespace Ahghee

open Ahghee.Grpc
open Ahghee.Grpc
open Google.Protobuf
open Google.Protobuf.Collections
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
open Ahghee.Grpc
open Google.Protobuf.WellKnownTypes
open Mono.Unix.Native

type ClusterServices(log: string -> unit) = 
    let remotePartitions = new ConcurrentDictionary<int,FileStorePartition>()
    
    let ByteToHex (bytes:byte[]) =
        bytes |> Array.fold (fun state x-> state + sprintf "%02X" x) ""
    member this.RemotePartitions() = remotePartitions
    interface IClusterServices with 
        member this.RemoteLookup (partition:int) (nid:NodeID) : bool * MemoryPointer = 
            if remotePartitions.ContainsKey partition then 
                let remote = remotePartitions.[ partition ]
                let mutable refPointers :Pointers = null
                let rind = remote.Index()
                if rind.TryGetValue(nid, & refPointers) then
                    true, refPointers.Pointers_ |> Seq.head
                else
                    false, Utils.NullMemoryPointer()
                
            else false, Utils.NullMemoryPointer()    
            


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
        
    let mergeNodesById (node:Node[]) =
        node
        |> Seq.groupBy(fun n -> n.Id)
        |> Seq.map(fun (m1,m2) -> m2 |> Seq.reduce(fun i1 i2 ->
                                                        i1.MergeFrom(i2)
                                                        let noDuplicates = i1.Attributes.Distinct().ToList()
                                                        i1.Attributes.Clear()
                                                        i1.Attributes.AddRange(noDuplicates)
                                                        i1))
        |> Seq.head
    
    let setTimestamps (node:Node) (nowInt:Int64) =
        for kv in node.Attributes do
            kv.Key.Timestamp <- nowInt
            kv.Value.Timestamp <- nowInt
            if kv.Key.Data.DataCase = DataBlock.DataOneofCase.Nodeid then
                kv.Key.Data.Nodeid.Pointer <- Utils.NullMemoryPointer()
            if kv.Value.Data.DataCase = DataBlock.DataOneofCase.Nodeid then
                kv.Value.Data.Nodeid.Pointer <- Utils.NullMemoryPointer()    
    
    let Flush () =
        ()
    
    let DataBlockCMP (left:DataBlock, op:String, right:DataBlock) =
        match op with
            | "==" -> left = right
            | "<" -> left < right
            | ">" -> left > right
            | "<=" -> left <= right
            | ">=" -> left >= right
            | _ -> raise <| Exception (sprintf "Operation not supported op %s" op)
    
    let RemoveFieldsNode (fieldsOp:FieldsOperator, node:Node) : Node =
        let isMatch(part: FieldsOperator.Types.CludeOp.Types.CludePart, data: DataBlock):bool =
            match part.PartCase with
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.IsCaret -> data.DataCase = DataBlock.DataOneofCase.Nodeid
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.IsStar -> true
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.IsTypeFloat -> data.DataCase = DataBlock.DataOneofCase.F
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.IsTypeInt -> data.DataCase = DataBlock.DataOneofCase.I32
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.IsTypeString -> data.DataCase = DataBlock.DataOneofCase.Str
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.StringMatch -> data.DataCase = DataBlock.DataOneofCase.Str && data.Str = part.StringMatch
            | FieldsOperator.Types.CludeOp.Types.CludePart.PartOneofCase.CarrotStringMatch -> data.DataCase = DataBlock.DataOneofCase.Nodeid && data.Nodeid.Iri = part.CarrotStringMatch
        let rec trimIt(clude:FieldsOperator.Types.Clude, worknWith: IEnumerable<KeyValue>) : IEnumerable<KeyValue> =
            match clude.OperatorCase with
            | FieldsOperator.Types.Clude.OperatorOneofCase.None -> Enumerable.Empty()
            | FieldsOperator.Types.Clude.OperatorOneofCase.List ->
                clude.List.Cludes
                |> Seq.collect (fun c -> trimIt(c, worknWith))
            | FieldsOperator.Types.Clude.OperatorOneofCase.Include -> worknWith.Union(trimIt(clude.Include, node.Attributes))
            | FieldsOperator.Types.Clude.OperatorOneofCase.Exclude -> worknWith.Except(trimIt(clude.Exclude, worknWith))
            | FieldsOperator.Types.Clude.OperatorOneofCase.Twoclude ->
                match clude.Twoclude.RightCase with
                | FieldsOperator.Types.TwoClude.RightOneofCase.None -> trimIt(clude.Twoclude.Left, worknWith)
                | FieldsOperator.Types.TwoClude.RightOneofCase.Include -> trimIt(clude.Twoclude.Left, worknWith).Union(trimIt(clude.Twoclude.Include, worknWith))
                | FieldsOperator.Types.TwoClude.RightOneofCase.Exclude -> trimIt(clude.Twoclude.Left, worknWith).Except(trimIt(clude.Twoclude.Exclude, worknWith))
                
            | FieldsOperator.Types.Clude.OperatorOneofCase.Op ->
                worknWith
                    |> Seq.filter (fun a ->
                                        let op = clude.Op
                                        let leftMatch = isMatch(op.Left, a.Key.Data)
                                        let rightMatch = isMatch(op.Right, a.Value.Data)
                                        leftMatch && rightMatch
                        )
        let newAttrs = trimIt (fieldsOp.Clude, node.Attributes) |> List.ofSeq
        node.Attributes.Clear()
        node.Attributes.AddRange(newAttrs)
        node
    
    let rec FilterNode (node:Node, step: Step) =
        let rec _filterNode (node:Node, cmp: FilterOperator.Types.Compare) =
            match cmp.CmpTypeCase with
                | FilterOperator.Types.Compare.CmpTypeOneofCase.KevValueCmp ->
                    node.Attributes
                        |> Seq.exists (fun kv ->
                                        kv.Key.Data = cmp.KevValueCmp.Property
                                            && DataBlockCMP (kv.Value.Data, cmp.KevValueCmp.MATHOP, kv.Value.Data)
                                        )
                | FilterOperator.Types.Compare.CmpTypeOneofCase.CompoundCmp ->
                    match cmp.CompoundCmp.BOOLOP with
                        | "&&" -> _filterNode(node, cmp.CompoundCmp.Left) && _filterNode(node, cmp.CompoundCmp.Right)
                        | "||" -> _filterNode(node, cmp.CompoundCmp.Left) || _filterNode(node, cmp.CompoundCmp.Right)
                        | _ -> raise <| Exception (sprintf "Operation not supported op %s" cmp.CompoundCmp.BOOLOP)
                | FilterOperator.Types.Compare.CmpTypeOneofCase.None -> true // ignore
                | _ -> true // shouldn't happen
                
        match step with
            | null -> true
            | s when s.OperatorCase = Step.OperatorOneofCase.Filter ->
                let cmp = s.Filter.Compare
                match _filterNode(node, cmp) with
                    | true -> FilterNode(node, step.Next)
                    | false ->  false
            | s -> true
    
    let rec EdgeCmpForFollow (dataBlock:DataBlock, follow: FollowOperator) =
        let rec _EdgeCmp (dataBlock:DataBlock, cmp: FollowOperator.Types.EdgeNum) =
            match cmp.OpCase with
                | FollowOperator.Types.EdgeNum.OpOneofCase.EdgeRange -> 
                    dataBlock = cmp.EdgeRange.Edge
                | FollowOperator.Types.EdgeNum.OpOneofCase.EdgeCmp ->
                    match cmp.EdgeCmp.BOOLOP with
                        | "&&" -> _EdgeCmp(dataBlock, cmp.EdgeCmp.Left) && _EdgeCmp(dataBlock, cmp.EdgeCmp.Right)
                        | "||" -> _EdgeCmp(dataBlock, cmp.EdgeCmp.Left) || _EdgeCmp(dataBlock, cmp.EdgeCmp.Right)
                        | _ -> raise <| Exception (sprintf "Operation not supported op %s" cmp.EdgeCmp.BOOLOP)
                | _ -> false
        
        match follow.FollowCase with
            | FollowOperator.FollowOneofCase.None -> false
            | FollowOperator.FollowOneofCase.FollowAny ->
                // the range value is shifted as we process through the operations
                // for us to be included, we must be in the current range from (from <= x <= 1)
                follow.FollowAny.Range.From <= 0 && follow.FollowAny.Range.To >= 0  
            | FollowOperator.FollowOneofCase.FollowEdge ->
                follow.FollowAny.Range.From <= 0 && follow.FollowAny.Range.To >= 0 && _EdgeCmp(dataBlock, follow.FollowEdge)
            | _ -> false
    
    let rec FollowStepDecrement(follow : FollowOperator) =
        let _decrRange (r:Range) =
                r.From <- max 0 (r.From - 1)
                r.To <- r.To - 1
        let rec _EdgeCmpDecr(fe : FollowOperator.Types.EdgeNum) =
            match fe.OpCase with
                    | FollowOperator.Types.EdgeNum.OpOneofCase.EdgeRange ->
                        _decrRange(fe.EdgeRange.Range)
                    | FollowOperator.Types.EdgeNum.OpOneofCase.EdgeCmp ->
                        _EdgeCmpDecr (fe.EdgeCmp.Left)
                        _EdgeCmpDecr (fe.EdgeCmp.Right)
                    | _ -> ()
                    
        match follow.FollowCase with
            | FollowOperator.FollowOneofCase.None -> ()
            | FollowOperator.FollowOneofCase.FollowEdge -> _EdgeCmpDecr follow.FollowEdge
            | FollowOperator.FollowOneofCase.FollowAny -> _decrRange follow.FollowAny.Range    

    
    let rec ContinueThisFollowStep(follow: FollowOperator) =
        let rec _edgeCmpValid(edgeNum: FollowOperator.Types.EdgeNum) =
            match edgeNum.OpCase with
                | FollowOperator.Types.EdgeNum.OpOneofCase.EdgeRange -> 
                    edgeNum.EdgeRange.Range.To > 0
                | FollowOperator.Types.EdgeNum.OpOneofCase.EdgeCmp ->
                    _edgeCmpValid (edgeNum.EdgeCmp.Left) &&
                        _edgeCmpValid (edgeNum.EdgeCmp.Right)
                | _ -> false
                
        match follow.FollowCase with
            | FollowOperator.FollowOneofCase.None -> false
            | FollowOperator.FollowOneofCase.FollowEdge -> _edgeCmpValid follow.FollowEdge
            | FollowOperator.FollowOneofCase.FollowAny -> follow.FollowAny.Range.To > 0
    let rec MergeSameSteps(step:Step)=
        match step.Next with
            | null -> step
            | next when next.OperatorCase <> step.OperatorCase -> step
            | next when step.OperatorCase = Step.OperatorOneofCase.None -> step
            | next when step.OperatorCase = Step.OperatorOneofCase.Skip ->
                let maxSkip = new Step()
                maxSkip.Skip <- new SkipFilter()
                maxSkip.Skip.Value <- max step.Skip.Value next.Skip.Value
                maxSkip.Next <- next.Next
                MergeSameSteps maxSkip
            | next when step.OperatorCase = Step.OperatorOneofCase.Limit ->
                let minLimit = new Step()
                minLimit.Limit <- new LimitFilter()
                minLimit.Limit.Value <- min step.Limit.Value next.Limit.Value
                minLimit.Next <- next.Next
                MergeSameSteps minLimit
            | next when step.OperatorCase = Step.OperatorOneofCase.Skip -> step
            | next when step.OperatorCase = Step.OperatorOneofCase.Filter ->
                let andedFilter = new Step()
                andedFilter.Filter <- new FilterOperator()
                andedFilter.Filter.Compare <- new FilterOperator.Types.Compare()
                andedFilter.Filter.Compare.CompoundCmp <- new FilterOperator.Types.CompareCompound()
                andedFilter.Filter.Compare.CompoundCmp.BOOLOP <- "&&"
                andedFilter.Filter.Compare.CompoundCmp.Left <- step.Filter.Compare
                andedFilter.Filter.Compare.CompoundCmp.Right <- next.Filter.Compare
                andedFilter.Next <- next.Next
                MergeSameSteps andedFilter
            | next when step.OperatorCase = Step.OperatorOneofCase.Follow ->
                match step.Follow.FollowCase, next.Follow.FollowCase with
                    | (_, FollowOperator.FollowOneofCase.FollowAny) ->
                        // any and any is still any, just skip this one.
                        MergeSameSteps next 
                    | (FollowOperator.FollowOneofCase.FollowAny, FollowOperator.FollowOneofCase.FollowEdge) ->
                        // any and an edge, is still and any, skip the next one
                        step.Next <- next.Next
                        MergeSameSteps step
                    | (FollowOperator.FollowOneofCase.FollowEdge, FollowOperator.FollowOneofCase.FollowEdge) ->
                        let andedFilter = new Step()
                        andedFilter.Follow <- new FollowOperator()
                        andedFilter.Follow.FollowEdge <- new FollowOperator.Types.EdgeNum()
                        andedFilter.Follow.FollowEdge.EdgeCmp <- new FollowOperator.Types.EdgeCMP()
                        andedFilter.Follow.FollowEdge.EdgeCmp.BOOLOP <- "&&"
                        andedFilter.Follow.FollowEdge.EdgeCmp.Left <- step.Follow.FollowEdge
                        andedFilter.Follow.FollowEdge.EdgeCmp.Left <- next.Follow.FollowEdge
                        andedFilter.Next <- next.Next
                        MergeSameSteps andedFilter
                    | (_,_) -> step
            | _ -> step
                
    let rec ApplyPaging(operation:Step, s  ) =
        match operation with
            | null -> s
            | op when op.OperatorCase = Step.OperatorOneofCase.Skip ->
                    ApplyPaging (operation.Next, s |> Seq.skip op.Skip.Value)
            | op when op.OperatorCase = Step.OperatorOneofCase.Limit ->
                    ApplyPaging (operation.Next, s |> Seq.truncate op.Limit.Value)
            | _ -> s
            
            
    let StartQueryNodeId (nid:NodeID) =
        let tcs = TaskCompletionSource<Node[]>()
        let partition = Utils.GetPartitionFromHash config.ParitionCount nid
        // this line is just plain wrong, we don't have a pointer with any of this data here.
        // if we did, then this would be ok to go I think.
        // Console.WriteLine("About to query shard "+ partition.ToString())
        let (bc,t,part) = PartitionWriters.[int <| partition]
        
        // TODO: Read all the fragments, not just the first one.
        let t = 
            if (nid.Pointer = Utils.NullMemoryPointer()) then
                //Console.WriteLine ("Read using Index")
                let mutable mp:Pointers = null
                if(part.Index().TryGetValue(nid, &mp)) then 
                    while bc.Writer.TryWrite (Read(tcs, mp.Pointers_ |> Array.ofSeq)) = false do ()
                    tcs.Task
                else 
                    tcs.SetException(new KeyNotFoundException("Index of NodeID -> MemoryPointer: did not contain the NodeID " + nid.Iri)) 
                    tcs.Task   
            else 
                Console.WriteLine ("Read using Pointer")
                while bc.Writer.TryWrite (Read(tcs, [|nid.Pointer|])) = false do ()
                tcs.Task
                
        let res = t.ContinueWith(fun (isdone:Task<Node[]>) ->
                    if (isdone.IsCompletedSuccessfully) then
                        config.Metrics.Measure.Meter.Mark(Metrics.FileStoreMetrics.ItemFragmentMeter)
                        nid, Either<Node,Exception>(isdone.Result |> mergeNodesById)
                    else 
                        nid, Either<Node,Exception>(isdone.Exception :> Exception)
                    )
        res |> Async.AwaitTask
    
    let StartScan (nid:NodeID): seq<Async<NodeID * Either<Node,Exception>>> =
        seq {
                let req =
                    seq {
                            for bc,t,part in PartitionWriters do
                                yield part.Index().Iter()
                                    |> Seq.map(fun ptrs ->
                                            let tcs = new TaskCompletionSource<Node[]>()
                                            let written = bc.Writer.WriteAsync(Read(tcs, ptrs.Pointers_.ToArray()))
                                            written, tcs.Task)
                        } |> Array.ofSeq
                            

                for (written, result) in req |> Seq.collect(fun x -> x) do
                    let finishup = async {
                        let! w = written.AsTask() |> Async.AwaitTask
                        let! loaded = result |> Async.AwaitTask
                        return (nid, Either<Node,Exception>( loaded |> mergeNodesById))
                    }
                    yield finishup                
            }
        
    let LoadNode(addressBlock:seq<NodeID>, filter:BloomFilter.Filter<int>) : seq<struct(NodeID * Either<Node, Exception>)> =
        let requestsMade =
            addressBlock
            |> Seq.distinct
            |> Seq.filter (fun ab ->
                if ab.Iri = "*" then // match anything
                    true
                else
                    let hash = ab.GetHashCode()
                    let f = filter.Contains(hash) |> not
                    filter.Add(hash) |> ignore
                    f
                )
            |> Seq.collect (fun nid ->
                if nid.Iri = "*" then
                    StartScan nid 
                else
                    [ StartQueryNodeId nid ] |> Seq.ofList 
                )
            
        seq {                            
            for ts in requestsMade |> Seq.map ( fun future -> future |> Async.RunSynchronously) do
                let (ab,eith) = ts
                yield (ab,eith)
            }
    
    let rec QueryNodes(addressBlock:seq<NodeID>, step: Step, filter:BloomFilter.Filter<int>) : seq<struct(NodeID * Either<Node, Exception>)> =        
        LoadNode(addressBlock, filter)
        |> (fun f ->
                    let mutable flow = f
                    let mutable stepper = step
                    while stepper <> null do
                        let pflow = flow
                        match stepper.OperatorCase with
                            | Step.OperatorOneofCase.Filter ->
                                let myStepper = stepper
                                flow <- pflow
                                        |> Seq.filter (fun struct(nid, item) ->
                                                                    if item.IsLeft = true then
                                                                        FilterNode(item.Left, myStepper)
                                                                    else
                                                                        true
                                                                        )
                            | Step.OperatorOneofCase.Skip           
                            | Step.OperatorOneofCase.Limit ->
                                flow <- ApplyPaging( stepper, pflow)
                            | Step.OperatorOneofCase.Fields ->
                                let myStepper = stepper
                                flow <- pflow                                                                                        
                                          |> Seq.map (fun struct(nid,item) ->
                                                                if item.IsLeft = true then
                                                                    let newNode =  RemoveFieldsNode(myStepper.Fields, item.Left)
                                                                    struct(nid, Either<Node,Exception>(newNode))
                                                                else
                                                                    struct(nid,item)
                                                                    )
                            | Step.OperatorOneofCase.Follow ->
                                let myStepper = stepper.Clone()
                                FollowStepDecrement(myStepper.Follow)
                                let continueFollowOrNextStep = 
                                                        match ContinueThisFollowStep(myStepper.Follow) with
                                                            | true -> myStepper
                                                            | false -> myStepper.Next
                                flow <-
                                        pflow
                                        |> Seq.collect (fun struct(nid,item) ->
                                            if item.IsLeft then
                                                 Seq.append ([struct(nid,item)]) (item.Left.Attributes
                                                                                  |> Seq.filter (fun attr ->
                                                                                        attr.Value.Data.DataCase = DataBlock.DataOneofCase.Nodeid
                                                                                        && EdgeCmpForFollow(attr.Key.Data, myStepper.Follow))
                                                                                  |> Seq.map (fun kv -> kv.Value.Data.Nodeid)
                                                                                  |> (fun nodeIds -> QueryNodes(nodeIds, continueFollowOrNextStep, filter)) )
                                            else
                                                [struct(nid,item)] |> Seq.ofList
                                            )
                            | _ -> flow <- pflow
                        stepper <- stepper.Next
                    flow
                    )
                    
    let QueryNoDuplicates(addressBlock:seq<NodeID>, step: Step) : System.Threading.Tasks.Task<seq<struct(NodeID * Either<Node, Exception>)>> =
        let filter = BloomFilter.FilterBuilder.Build<int>(10000)
        let mutable mergedSteps =
            if step <> null then
                MergeSameSteps step
            else
                Step()
        let nodeStream = QueryNodes(addressBlock, mergedSteps, filter)
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
    
    let GetStats(req:GetStatsRequest, cancel: CancellationToken): Task<GetStatsResponse> =
        
        Task.FromResult(GetStatsResponse())
    let GetMetrics(req:GetMetricsRequest, cancel: CancellationToken): Task<GetMetricsResponse> =
        collectSystemMetrics()
        let snap = config.Metrics.Snapshot
        let x = snap.Get()
        let ts = x.Timestamp
        let gmr = GetMetricsResponse()
        for context in x.Contexts do
            for counter in context.Counters do
                let met = GetMetricsResponse.Types.Metric()
                met.Name <- counter.Name
                met.Time <- Timestamp.FromDateTime ts
                met.Value <- float32 counter.Value.Count
                gmr.Metrics.Add (met)
            for guage in context.Gauges do
                let met = GetMetricsResponse.Types.Metric()
                met.Name <- guage.Name
                met.Time <- Timestamp.FromDateTime ts
                met.Value <- float32 guage.Value
                gmr.Metrics.Add (met)
            for meter in context.Meters do
                let met = GetMetricsResponse.Types.Metric()
                met.Name <- meter.Name
                met.Time <- Timestamp.FromDateTime ts
                met.Value <- float32 meter.Value.OneMinuteRate
                gmr.Metrics.Add (met)
            for hist in context.Histograms do
                let met = GetMetricsResponse.Types.Metric()
                met.Name <- hist.Name
                met.Time <- Timestamp.FromDateTime ts
                met.Value <- float32 hist.Value.Count
                gmr.Metrics.Add (met)      
        Task.FromResult(gmr)
    
    interface IDisposable with
        member x.Dispose() = Stop()
        
    interface IStorage with
    
        member x.GetStats(req, cancel) =
            GetStats(req, cancel)
        
        member x.GetMetrics(req, cancel) =
            GetMetrics(req,cancel)
        
        member x.Nodes() : IEnumerable<Node> = 
            // return local nodes before remote nodes
            // let just start by pulling nodes from the index.
            seq {
                let req =
                    seq {
                            for bc,t,part in PartitionWriters do
                                yield part.Index().Iter()
                                    |> Seq.map(fun ptrs ->
                                            let tcs = new TaskCompletionSource<Node[]>()
                                            let written = bc.Writer.WriteAsync(Read(tcs, ptrs.Pointers_.ToArray()))
                                            written, tcs.Task)
                        } |> Array.ofSeq
                            

                for (written, result) in req |> Seq.collect(fun x -> x) do
                    if written.IsCompletedSuccessfully then
                        result.Wait()
                        yield result.Result |> mergeNodesById
                                  
                                  
                    else 
                        written.AsTask().Wait()
                        result.Wait()
                        yield result.Result |> mergeNodesById
                
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

                Parallel.For(0,lstNodes.Length,(fun i ->
                    let node = lstNodes.[i]
                    while node.Fragments.Count < 3 do
                        node.Fragments.Add (Utils.NullMemoryPointer())
                    setTimestamps node nowInt
                    let partition = Utils.GetPartitionFromHash config.ParitionCount node.Id
                    let plist = partitionLists.[partition] 
                    lock (plist) (fun () -> plist.Add node) 
                )) |> ignore
               
                partitionLists
                    |> Seq.iteri (fun i list ->
                        if (list.Count > 0) then
                            let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)         
                            let (bc,_,_) = PartitionWriters.[i]
                            while bc.Writer.TryWrite ( Add(tcs,list)) = false do
                                Console.WriteLine "Couldn't Add"
                        )
                
                config.Metrics.Measure.Meter.Mark(Metrics.FileStoreMetrics.AddFragmentMeter, count)
                
                )
                
                        
        member x.Remove (nodes:seq<NodeID>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<NodeID>, follow: Step) = QueryNoDuplicates(addressBlock, follow)
        member x.First (predicate: Func<Node, bool>) = raise (new NotImplementedException())
        member x.Stop () =  
            Flush()
            for (bc,t,part) in PartitionWriters do
                bc.Writer.Complete()
                t.Join() // wait for shard threads to stop    
