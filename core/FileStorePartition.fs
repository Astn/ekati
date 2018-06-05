namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Linq
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc
open App.Metrics

type FileStorePartition(config:Config, i:int, cluster:IClusterServices) = 
    let tags = MetricTags([| "partition_id" |],
                          [| i.ToString() |]) 
        

    // TODO: These next two indexes may be able to be specific to a particular thread writer, and then they wouldn't need to be concurrent if that is the case.
    // The idea behind this index is to know which connections we do not need to make as they are already made
    let FragmentLinksConnected = new System.Collections.Generic.Dictionary<Grpc.MemoryPointer, seq<Grpc.MemoryPointer>>()
    // The idea behind this index is to know which connections we know we need to make that we have not yet made
    let FragmentLinksRequested = new System.Collections.Generic.Dictionary<Grpc.MemoryPointer, seq<Grpc.MemoryPointer>>()
    
    // TODO: rename to IORequests
    let bc = new System.Collections.Concurrent.BlockingCollection<NodeIO>()
        
    let NodeIdFromAddress (addr:AddressBlock) =
        match addr.AddressCase with 
        | AddressBlock.AddressOneofCase.Nodeid -> addr.Nodeid
        | AddressBlock.AddressOneofCase.Globalnodeid -> addr.Nodeid
        | _ ->  raise (new NotSupportedException("AddresBlock did not contain a valid address"))

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
    
    // returns true if the node is or was linked to the pointer
    // also updates indexes (FragmentLinksRequested; FragmentLinksConnected) to reflect changes, and request 
    // bidirectional linking for this fragment and the pointer it links to
    let LinkFragmentTo (n:Node) (mp:MemoryPointer) =
        if (mp.Partitionkey <> n.Id.Nodeid.Pointer.Partitionkey
            || mp.Filename <> n.Id.Nodeid.Pointer.Filename
            || mp.Offset <> n.Id.Nodeid.Pointer.Offset) then 
            
            // Find the first null pointer and update it.
            // We don't use ADD because that would change the memory size for this fragment. 
            let mutable previouslyAttached = false
            let mutable attached = false 
            let mutable i = 0
            while not previouslyAttached && not attached && i < n.Fragments.Count do
                
                previouslyAttached <- 
                    if n.Fragments.Item(i) = mp then true
                    else false
                
                attached <- 
                    if n.Fragments.Item(i) = Utils.NullMemoryPointer() then 
                        n.Fragments.Item(i) <- mp

                        // Track that we have linked these fragments
                        if FragmentLinksConnected.ContainsKey(n.Id.Nodeid.Pointer) = false then 
                            FragmentLinksConnected.Item(n.Id.Nodeid.Pointer) <- [mp]
                        else
                            FragmentLinksConnected.Item(n.Id.Nodeid.Pointer) <- FragmentLinksConnected.Item(n.Id.Nodeid.Pointer) |> Seq.append [mp]   

                        // If this was a requested link, remove it from the requests
                        if FragmentLinksRequested.ContainsKey(n.Id.Nodeid.Pointer) && FragmentLinksRequested.Item(n.Id.Nodeid.Pointer) |> Seq.length > 1 then
                            FragmentLinksRequested.Item(n.Id.Nodeid.Pointer) <- FragmentLinksRequested.Item(n.Id.Nodeid.Pointer) |> Seq.except [mp]
                        else if FragmentLinksRequested.ContainsKey(n.Id.Nodeid.Pointer) then  
                            FragmentLinksRequested.Remove(n.Id.Nodeid.Pointer) |> ignore
                            
                                                   
                        // Track that we want to link it from the other direction, but only if that hasn't already been done
                        if FragmentLinksConnected.ContainsKey(mp) = false && FragmentLinksRequested.ContainsKey(mp) = false then 
                            FragmentLinksRequested.Add(mp, [n.Id.Nodeid.Pointer]) 
                        else if FragmentLinksRequested.ContainsKey(mp) && FragmentLinksRequested.Item(mp) |> Seq.contains n.Id.Nodeid.Pointer = false then
                            FragmentLinksRequested.Item(mp) <- FragmentLinksRequested.Item(mp) |> Seq.append [n.Id.Nodeid.Pointer] 

                        true
                    else 
                        false
                i <- i + 1
            previouslyAttached || attached
        else false

    // Right now this function will update the fragment pointers inside the passed in node to link to
    // any fragments in the FragmentLinksRequested index -OR- it will link it to the fragment at the end of the
    // ``Index of NodeID -> MemoryPointer`` index.
    // NOTE: It will also update the FragmentLinksRequested with bi-directional links that are still needed
    let LinkFragments (n:Node) =
        try
            let hash = n.Id |> NodeIdFromAddress |> Utils.GetNodeIdHash
            // If this node has links requested, then make those and exit
            let mutable outMp:seq<MemoryPointer> = Seq.empty<MemoryPointer>
            if FragmentLinksRequested.TryGetValue(n.Id.Nodeid.Pointer, & outMp) && outMp |> Seq.length > 0 then 
                seq {
                    for mp in outMp do
                        if LinkFragmentTo n mp then  
                            yield mp 
                    }
            // otherwise link it to something at the end of the NodeIdToPointers index
            else if (``Index of NodeID -> MemoryPointer``.TryGetValue (hash, & outMp)) &&
                not (n.Fragments |> Seq.exists(fun frag -> frag <> Utils.NullMemoryPointer())) then
                // we want to create a basic linked list. 
                let pointers = outMp |> List.ofSeq
                let mutable linked = false 
                let mutable i = 0
                let mutable mp = Utils.NullMemoryPointer()
                while pointers.Length > 1 && not linked && i < pointers.Length do
                    mp <- pointers.Item(i)
                    linked <- LinkFragmentTo n mp
                    i <- i + 1
                if linked then 
                    [mp] |> Seq.ofList
                else Seq.empty
            else Seq.empty
        with
        | e -> 
            config.log (sprintf "Errors: %A" e)
            raise e  
              
    let UpdateMemoryPointers (node:Node)=
        let AttachMemoryPointer (nodeid:NodeID) =
            if (nodeid.Pointer.Length = uint64 0) then
                let hash = Utils.GetNodeIdHash nodeid
                let partition = Utils.GetPartitionFromHash config.ParitionCount hash
                if partition = i then // we have the info local
                    let mutable outMp:seq<MemoryPointer> = Seq.empty<MemoryPointer>
                    if (``Index of NodeID -> MemoryPointer``.TryGetValue (Utils.GetNodeIdHash nodeid, & outMp)) then
                        nodeid.Pointer <- outMp |> Seq.head
                        true,true
                    else
                        true,false
                else // its a remote node and we need to ask the cluster for the info.
                    let success,pointer = cluster.RemoteLookup partition hash 
                    if success then
                        nodeid.Pointer <- pointer 
                        true,true
                    else
                        true,false 
            else 
                false,false    
             
        let updated = 
            node.Attributes
            |> Seq.collect (fun attr -> 
                attr.Value
                |> Seq.append [|attr.Key|])
            |> Seq.map (fun tmd ->
                            match  tmd.Data.DataCase with
                            | DataBlock.DataOneofCase.Address ->
                                tmd.Data.Address |> NodeIdFromAddress |> AttachMemoryPointer
                                | _ -> false,false)
        let anyChanged = 
            updated |> Seq.contains (true,true)
        let anyMissed =
            updated |> Seq.contains (true,false)                    
        anyChanged, anyMissed

    
    
    let BuildGroups (fpwb:SortedList<uint64,MemoryPointer>) = 
        let groups = new List<NodeIOGroup>()
        let mutable blockStarted = false
        let mutable start =uint64 0
        let mutable length = uint64 0
        let mutable blockItems = new List<MemoryPointer>()
        for i in 0 .. fpwb.Count - 1 do
            let key = fpwb.Keys.[i]
            let values = fpwb.Values.[i]
            if (blockStarted = true && length <> key) then
               // we go here cause this item is not the next contigious one.
               // close out the current block and start a new one
               groups.Add { start=start;length=length;items=blockItems }
               blockStarted <- false 
               start <- uint64 0                 
               length <- uint64 0
               blockItems <- new List<MemoryPointer>() 
        
            if (blockStarted = false) then
                blockStarted <- true 
                start <- key
                length <- length + values.Length
                blockItems.Add values    
            else // this **must** be the next contigious space in memory because the earlier if forces it
                length <- length + values.Length 
                blockItems.Add values
        
        // close out last group
        if (blockItems.Count > 0) then
            groups.Add { start=start;length=length;items=blockItems }
        
        groups    

    // returns true if we flushed or moved the position in the out or filestream
    let writeGroups groups ofSize (stream:FileStream) (op:WriteGroupsOperation) : bool * IOStat = 
        let mutable readBytes = 0UL
        let mutable writeBytes = 0UL
        let flushOrMoved =
            seq {for g in groups do
                    // Any group over size we will write out.
                 yield if (float g.length >= ofSize) then
                            // gotta flush before we try to read data we put in it.
                            stream.Flush()
                            stream.Position <- int64 g.start
                            
                            let writeback = new List<MemoryPointer * Node>()
                            
                            // do all the reading
                            let mutable lastex:Exception=null
                            for nid in g.items do
    
                                match nid with 
                                | _ when int64 nid.Offset = stream.Position -> 
                                    lastex <- null
                                    let toFix = new Node()
                                    let buffer = Array.zeroCreate<byte> (int nid.Length)
        
                                    let readResult = stream.Read(buffer,0,int nid.Length)
                                    readBytes <- readBytes + nid.Length
                                    if(readResult <> int nid.Length || buffer.[0] = byte 0) then
                                        raise (new Exception("wtf"))
                                    MessageExtensions.MergeFrom(toFix,buffer,0,int nid.Length)
                                    // now fix it.
                                    
                                    let linksChanged = 
                                        if op &&& WriteGroupsOperation.LinkFragments = WriteGroupsOperation.LinkFragments then 
                                            (LinkFragments toFix
                                                |> Seq.length)
                                                > 0
                                        else false
                                        
                                    let (anyChanged,anyMissed) = 
                                        if op &&& WriteGroupsOperation.FixPointers = WriteGroupsOperation.FixPointers then
                                            UpdateMemoryPointers toFix
                                        else (false, false)    
                                    
                                    if ((anyChanged || linksChanged) && toFix.CalculateSize() |> uint64 <> nid.Length) then
                                        raise (new Exception(sprintf "Updating MemoryPointer changed Node Size - before: %A after: %A" nid.Length (toFix.CalculateSize() |> uint64)))
                                    
                                    if (anyChanged || linksChanged) then    
                                        // !!!!do the writing later in a batch..
                                        writeback.Add (nid, toFix)
                                | _ when int64 nid.Offset < stream.Position -> 
                                    // this should be because its a duplicate request for the previous item
                                    ()
                                | _ when int64 nid.Offset > stream.Position ->
                                    // this should not happen
                                    raise (new InvalidOperationException("The block of FixPointers was not contigious"))
                                | _ -> 
                                    raise (new InvalidOperationException("How did this happen?"))          
                            // do all the writing
                            if (writeback.Count > 0) then 
                                stream.Position <- int64 g.start
                                let flatArray = Array.zeroCreate<byte>(int g.length)
                                let oout = new CodedOutputStream(flatArray) 
                                for wb in writeback do
                                    let req,n = wb
                                    n.WriteTo oout
                                
                                oout.Flush()
                                if (stream.Position <> (int64 g.start)) then
                                    stream.Position <- int64 g.start
                                stream.Write(flatArray,0,flatArray.Length)   
                                writeBytes <- writeBytes + uint64 flatArray.LongLength 
                                stream.Flush() 
                               
                            true
                        else 
                            false}
            |> Seq.contains true   
        flushOrMoved, { IOStat.readbytes=readBytes; IOStat.writebytes=writeBytes }             
                    
    let IOThread =  
        let t = new ThreadStart((fun () -> 
            // TODO: If we cannot access this file, we need to mark this parition as offline, so it can be written to remotely
            // TODO: log file access failures
            
            let dir = match config.CreateTestingDataDirectory with 
                      | true -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("data-"+ Path.GetRandomFileName())))
                      | false -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data"))
            let fileNameid = i 
            let fileName = Path.Combine(dir.FullName, (sprintf "ahghee.%i.tmp" i))
            let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024*10000,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
            // PRE-ALLOCATE the file to reduce fragmentation https://arxiv.org/pdf/cs/0502012.pdf
            let PreAllocSize = int64 (1024 * 100000 )
            stream.SetLength(PreAllocSize)
            let out = new CodedOutputStream(stream)
            let FixPointersWriteBuffer = new SortedList<uint64,MemoryPointer>()
            let mutable lastOpIsWrite = false
            let mutable lastPosition = 0L 
            let FLUSHWRITES () =   
                if (out.Position > stream.Position) then
                    out.Flush()
                    stream.Flush()
            try
                for nio in bc.GetConsumingEnumerable() do
                    match nio with
                    | Add(tcs,items) -> 
                        try
                            use writeTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.AddTimer, tags)

                            if (lastOpIsWrite = false) then
                                stream.Position <- lastPosition
                                lastOpIsWrite <- true
                            let startPos = out.Position
                            let mutable count = 0L
                            
                            let mutable ownOffset = uint64 startPos
                            
                            for item in items do
                                let mp = item.Id.Nodeid.Pointer
                                mp.Partitionkey <- uint32 i
                                mp.Filename <- uint32 fileNameid
                                mp.Offset <- ownOffset
                                mp.Length <- (item.CalculateSize() |> uint64)
                                ownOffset <- ownOffset + mp.Length
                                count <- count + 1L                                   

                            for item in items do        
                                item.WriteTo out
                                
                            for item in items do    
                                let id = item.Id.Nodeid
                                if (not (FixPointersWriteBuffer.ContainsKey id.Pointer.Offset)) then
                                    FixPointersWriteBuffer.Add(id.Pointer.Offset,id.Pointer)
                                else
                                    ()
                             
                            for item in items do
                                let id = item.Id.Nodeid                                                                                           
                                IndexMaintainer.Post (Index(id))

                            lastPosition <- out.Position
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddFragmentMeter, tags, count)
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.AddSize, tags, lastPosition - startPos)
                            tcs.SetResult()
                            
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | Read (tcs,request) ->  
                        try 
                            use ReadTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.ReadTimer, tags)
                            
                            FLUSHWRITES()                          
                            lastOpIsWrite <- false
                            stream.Position <- int64 request.Offset
                            
                            let nnnnnnnn = new Node()
                            let buffer = Array.zeroCreate<byte> (int request.Length)
                            
                            let readResult = stream.Read(buffer,0,int request.Length)
                            if(readResult <> int request.Length ) then
                                raise (new Exception("wtf"))
                            if(buffer.[0] = byte 0) then
                                raise (new Exception(sprintf "Read null data @ %A - %A\n%A" fileName request buffer))    
                            
                            MessageExtensions.MergeFrom(nnnnnnnn,buffer,0,int request.Length)
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.AddSize, tags, int64 request.Length)
                            tcs.SetResult(nnnnnnnn)
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | FlushFixPointers (tcs) ->
                        try
                            use FlushFixPointersTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.FlushFixPointersTimer, tags)
                            
                            FLUSHWRITES()    
                            let groups = BuildGroups FixPointersWriteBuffer        
                            FixPointersWriteBuffer.Clear()
                            
                            let reply = IndexMaintainer.PostAndReply(Flush)
                            
                            let anySize = float 0
                            let movedOrFlushed, iostat =writeGroups groups anySize stream (WriteGroupsOperation.FixPointers ||| WriteGroupsOperation.LinkFragments) 
                            if movedOrFlushed then
                                lastOpIsWrite <- false
                            
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.FlushFixPointersWriteSize, tags, int64 iostat.writebytes)
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.FlushFixPointersReadSize, tags, int64 iostat.readbytes)    
                            tcs.SetResult()  
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)        
                    | FlushAdds (tcs) ->
                        try 
                            use FlushAddsTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.FlushAddsTimer, tags)
                            FLUSHWRITES()
                            tcs.SetResult()
                        with 
                        | ex ->
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)      
                    | FlushFragmentLinks (tcs) ->
                        try 
                            use FlushFragmentLinksTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.FlushFragmentLinksTimer, tags)
                            FLUSHWRITES()
                            // And read, and update each of those fragments calling LinkFragments.
                            
                            
                            let reply = IndexMaintainer.PostAndReply(Flush)
                            
                            // Try to use a single pass to update everything                           
                            // for everything in FragmentLinksRequested, If it's inverse is not already contained
                            // in FragmentLinksConnected, then insert that inverse into FragmentLinksRequested
                            let toAdd = 
                                seq {
                                    for flr in FragmentLinksRequested do
                                        for subflr in flr.Value do
                                            if FragmentLinksConnected.ContainsKey(subflr) = false ||
                                               FragmentLinksConnected.Item(subflr).Contains(flr.Key) = false then
                                               yield subflr , flr.Key
                                    }           
                            
                            for k,v in toAdd do
                                if FragmentLinksRequested.ContainsKey(k) = false then
                                    FragmentLinksRequested.Add(k,[v])
                                else if FragmentLinksRequested.Item(k).Contains(v) = false then
                                    FragmentLinksRequested.Item(k) <- FragmentLinksRequested.Item(k).Concat [v] 
                            
                            let groupsInput = new SortedList<uint64,MemoryPointer>() 
                            for mp in (FragmentLinksRequested.Keys 
                                        |> Seq.filter (fun x -> x.Partitionkey = uint32 i)
                                        |> Seq.sortBy (fun x -> x.Offset)) do
                                groupsInput.Add(mp.Offset, mp)
                            let groups = BuildGroups groupsInput    
                            
                            let anySize = float 0
                            let movedOrFlushed, iostat = writeGroups groups anySize stream WriteGroupsOperation.LinkFragments
                            if movedOrFlushed then
                                lastOpIsWrite <- false  
                            
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.FlushFragmentLinksWriteSize, tags, int64 iostat.writebytes)
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.FlushFragmentLinksReadSize, tags, int64 iostat.readbytes)
                                
                            tcs.SetResult()  
                        with 
                        | ex ->
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)                                           
            finally
                config.log <| sprintf "Shutting down partition writer[%A]" i 
                config.log <| sprintf "Flushing partition writer[%A]" i
                out.Flush()
                out.Dispose()
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
    member __.Index() = ``Index of NodeID -> MemoryPointer``