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
open Metrics

type FileStorePartition(config:Config, i:int, cluster:IClusterServices) = 
    let tags = MetricTags([| "partition_id" |],
                          [| i.ToString() |]) 
        
    let dir = match config.CreateTestingDataDirectory with 
                      | true -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("data-"+ Path.GetRandomFileName())))
                      | false -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data"))
    // TODO: These next two indexes may be able to be specific to a particular thread writer, and then they wouldn't need to be concurrent if that is the case.
    // The idea behind this index is to know which connections we do not need to make as they are already made
    let FragmentLinksConnected = new System.Collections.Generic.Dictionary<Grpc.MemoryPointer, List<Grpc.MemoryPointer>>()
    // The idea behind this index is to know which connections we know we need to make that we have not yet made
    let FragmentLinksRequested = new System.Collections.Generic.Dictionary<Grpc.MemoryPointer, List<Grpc.MemoryPointer>>()
    
    // TODO: rename to IORequests
    let bc = System.Threading.Channels.Channel.CreateBounded<NodeIO>(1000)
        
    let NodeIdFromAddress (addr:NodeID) = addr

    // TODO: Switch to PebblesDB when index gets to big
    // TODO: Are these per file, with bloom filters, or aross files in the same shard?
    let ``Index of NodeID -> MemoryPointer`` = new NodeIdIndex(Path.Combine(dir.FullName, if config.CreateTestingDataDirectory then Path.GetRandomFileName() else "nodeindex"+i.ToString()))
    
    let IndexNodeIds (nids:seq<NodeID>) =
        let lookup = 
            nids
            |> Seq.map(fun x ->
                let hash = x.GetHashCode()
                (hash |> BitConverter.GetBytes), x.Pointer)
            |> Map.ofSeq
             
        let keys =
            lookup |> Seq.map (fun (x)->x.Key) |> Array.ofSeq
        ``Index of NodeID -> MemoryPointer``.AddOrUpdateBatch nids 
       
    // returns true if the node is or was linked to the pointer
    // also updates indexes (FragmentLinksRequested; FragmentLinksConnected) to reflect changes, and request 
    // bidirectional linking for this fragment and the pointer it links to
    let LinkFragmentTo (n:Node) (mp:MemoryPointer) =
        if (mp.Partitionkey <> n.Id.Pointer.Partitionkey
            || mp.Filename <> n.Id.Pointer.Filename
            || mp.Offset <> n.Id.Pointer.Offset) then 
            
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
                    if n.Fragments.Item(i).Length = 0UL then 
                        n.Fragments.Item(i) <- mp

                        // Track that we have linked these fragments
                        if FragmentLinksConnected.ContainsKey(n.Id.Pointer) = false then 
                            FragmentLinksConnected.Item(n.Id.Pointer) <- 
                                let lst = new System.Collections.Generic.List<MemoryPointer>()
                                lst.Add mp
                                lst
                        else
                            FragmentLinksConnected.Item(n.Id.Pointer) <- 
                                let lst = FragmentLinksConnected.Item(n.Id.Pointer)
                                lst.Add(mp)
                                lst   

                        // If this was a requested link, remove it from the requests
                        if FragmentLinksRequested.ContainsKey(n.Id.Pointer) && FragmentLinksRequested.Item(n.Id.Pointer).Count > 1 then
                            FragmentLinksRequested.Item(n.Id.Pointer) <- 
                                let lst = FragmentLinksRequested.Item(n.Id.Pointer)
                                lst.Remove mp |> ignore
                                lst
                        else if FragmentLinksRequested.ContainsKey(n.Id.Pointer) then  
                            FragmentLinksRequested.Remove(n.Id.Pointer) |> ignore
                            
                                                   
                        // Track that we want to link it from the other direction, but only if that hasn't already been done
                        if (FragmentLinksConnected.ContainsKey(mp) = false || FragmentLinksConnected.Item(mp).Contains(n.Id.Pointer) = false) then
                            if FragmentLinksRequested.ContainsKey(mp) = false then
                                let lst = new System.Collections.Generic.List<MemoryPointer>()
                                lst.Add n.Id.Pointer 
                                FragmentLinksRequested.Add(mp, lst) 
                            else if FragmentLinksRequested.Item(mp).Contains(n.Id.Pointer) = false then
                                FragmentLinksRequested.Item(mp) <- 
                                    let lst = FragmentLinksRequested.Item(mp)
                                    lst.Add n.Id.Pointer
                                    lst 

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
            // If this node has links requested, then make those and exit
            let mutable outMpA:List<MemoryPointer> = null //System.Collections.Generic.List
            let mutable outMpB:Pointers = null 
            if FragmentLinksRequested.TryGetValue(n.Id.Pointer, & outMpA) && outMpA |> Seq.length > 0 then 
                seq {
                    for mp in outMpA do
                        if LinkFragmentTo n mp then  
                            yield mp 
                    }
            // otherwise link it to something at the end of the NodeIdToPointers index
            else if (``Index of NodeID -> MemoryPointer``.TryGetValue (n.Id, & outMpB)) &&
                not (n.Fragments |> Seq.exists(fun frag -> frag.Length <> 0UL)) then
                // we want to create a basic linked list. 
                let pointers = outMpB
                let mutable linked = false 
                let mutable i = pointers.Pointers_.Count - 1
                let mutable mp: MemoryPointer = null
                while not linked && i >= 0 do
                    mp <- pointers.Pointers_.Item(i)
                    linked <- LinkFragmentTo n mp
                    i <- i - 1
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
                
                let partition = Utils.GetPartitionFromHash config.ParitionCount nodeid
                if partition = i then // we have the info local
                    let mutable outMp:Pointers = null
                    if (``Index of NodeID -> MemoryPointer``.TryGetValue (nodeid, & outMp)) then
                        nodeid.Pointer <- outMp.Pointers_ |> Seq.head
                        true,true
                    else
                        true,false
                else // its a remote node and we need to ask the cluster for the info.
                    let success,pointer = cluster.RemoteLookup partition nodeid 
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
                [attr.Value; attr.Key])
            |> Seq.map (fun tmd ->
                            match  tmd.Data.DataCase with
                            | DataBlock.DataOneofCase.Nodeid ->
                                tmd.Data.Nodeid |> NodeIdFromAddress |> AttachMemoryPointer
                                | _ -> false,false)
        // todo: only iterate the updated seq once
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
        let arraypool = System.Buffers.ArrayPool<byte>.Shared
        
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
                                    let buffer = arraypool.Rent(int nid.Length)
        
                                    let readResult = stream.Read(buffer,0,int nid.Length)
                                    readBytes <- readBytes + nid.Length
                                    if(readResult <> int nid.Length || buffer.[0] = byte 0) then
                                        raise (new Exception("wtf"))
                                    MessageExtensions.MergeFrom(toFix,buffer,0,int nid.Length)
                                    arraypool.Return(buffer, true) // todo: does this need to be put in a finally block?
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
                                let flatArray = arraypool.Rent(int g.length)
                                let oout = new CodedOutputStream(flatArray) 
                                for wb in writeback do
                                    let req,n = wb
                                    n.WriteTo oout
                                
                                oout.Flush()
                                if (stream.Position <> (int64 g.start)) then
                                    stream.Position <- int64 g.start
                                stream.Write(flatArray,0, int g.length)   
                                writeBytes <- writeBytes + g.length
                                arraypool.Return(flatArray, true) // todo: does this need to be in a finally block? 
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
            
            
            let fileNameid = i 
            let fileName = Path.Combine(dir.FullName, (sprintf "ahghee.%i.tmp" i))
            let fileNamePos = Path.Combine(dir.FullName, (sprintf "ahghee.%i.pos" i))
            let fileNameScanIndex = Path.Combine(dir.FullName, (sprintf "ahghee.%i.scan.index" i))
            let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024*10000,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
            let scanIndexFile = new IO.FileStream(fileNameScanIndex,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024*10000,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
            
            
            // PRE-ALLOCATE the file to reduce fragmentation https://arxiv.org/pdf/cs/0502012.pdf
            let PreAllocSize = int64 (1024 * 100000 )
            stream.SetLength(PreAllocSize)
            
            let FixPointersWriteBuffer = new SortedList<uint64,MemoryPointer>()
            let arraybuffer = System.Buffers.ArrayPool<byte>.Shared 
            let mutable lastOpIsWrite = false
            let mutable lastPosition = 0L
            
           
            let loadLastPos () =
                // this cannot be called after we create a writer a few lines after we initially call loadLastPos
                // because we hold open the file.
                use posStream = new IO.FileStream(fileNamePos,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,8,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
                posStream.SetLength(int64 8)
                use br = new BinaryReader(posStream)
                lastPosition <- br.ReadInt64()
            loadLastPos()
            
            let posStream = new IO.FileStream(fileNamePos,IO.FileMode.Open,IO.FileAccess.Write,IO.FileShare.Read,8,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
            use bw = new BinaryWriter(posStream)                
            
            // todo: make this async
            let writeLastPos (pos : int64) =
                bw.Seek (0, SeekOrigin.Begin) |> ignore
                bw.Write (pos)

            let FLUSHWRITES () =   
                if lastOpIsWrite then 
                    stream.Flush()
                    writeLastPos(lastPosition)
            try
                let reader = bc.Reader
                let alldone = reader.Completion
                let myNoOp = NoOP()
                while alldone.IsCompleted = false do
                    let mutable nio: NodeIO = NodeIO.NoOP() 
                    if reader.TryRead(&nio) = false then
                        let nioWaitTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.QueueEmptyWaitTime, tags)
                        // sleep for now. later we will want do do compaction tasks
                        //printf "Waited."
                        System.Threading.Thread.Sleep(1) // sleep to save cpu
                        nio <- myNoOp // set NoOp so we can loop and check alldone.IsCompleted again.
                        nioWaitTimer.Dispose()    
                    match nio with
                    | Add(tcs,items) -> 
                        try
                            use writeTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.AddTimer, tags)
                            // Console.WriteLine("Adding to shard "+ fileNameid.ToString())
                            if (lastOpIsWrite = false) then
                                stream.Position <- lastPosition
                                lastOpIsWrite <- true
                            let startPos = lastPosition
                            let mutable count = 0L
                            let mutable batchLen = 0L
                            let mutable ownOffset = uint64 startPos
                            let x = System.IO.Pipelines.Pipe()
                            
                            for item in items do
                                let mp = item.Id.Pointer
                                mp.Partitionkey <- uint32 i
                                mp.Filename <- uint32 fileNameid
                                mp.Offset <- ownOffset
                                mp.Length <- (item.CalculateSize() |> uint64)
                                batchLen <- batchLen + int64 mp.Length
                                ownOffset <- ownOffset + mp.Length
                                count <- count + 1L                         
                                
                            let rentedBuffer = arraybuffer.Rent(int batchLen) 
                            let out = new CodedOutputStream(rentedBuffer)
                            for item in items do        
                                item.WriteTo out
                            
                            let copyTask = stream.WriteAsync(rentedBuffer,0,int out.Position)
                                
                            for item in items do    
                                let id = item.Id
                                if (not (FixPointersWriteBuffer.ContainsKey id.Pointer.Offset)) then
                                    FixPointersWriteBuffer.Add(id.Pointer.Offset,id.Pointer)
                                else
                                    ()
                             
                            items 
                                |> Seq.map (fun x -> x.Id) 
                                |> Array.ofSeq
                                |> Seq.ofArray
                                |> IndexNodeIds

                            lastPosition <- int64 ownOffset
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddFragmentMeter, tags, count)
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.AddSize, tags, lastPosition - startPos)
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddSizeBytes, tags, lastPosition - startPos)
                            copyTask.Wait()
                            arraybuffer.Return rentedBuffer
                            tcs.SetResult()
                            
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | Read (tcs,requests) ->  
                        try 
                            let ReadTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.ReadTimer, tags)
                            //Console.WriteLine("Reading from shard "+ fileNameid.ToString())
                            FLUSHWRITES()                          
                            lastOpIsWrite <- false
                            if requests.Length > 0 then
                                let batchStart = requests.[0].Offset
                                let batchEnd = requests.[requests.Length - 1].Offset + requests.[requests.Length - 1].Length 
                                let bufferLen = (batchEnd - batchStart)
                                let buffer = arraybuffer.Rent(int bufferLen)
                                
                                let readIOTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.ReadIOTimer, tags)
                                if stream.Position <> int64 batchStart then
                                    stream.Position <- int64 batchStart //TODO: make sure the offset is less than the end of the file
                                
                                // read a buffer that spans all of the data for every requested pointer
                                // TODO: this doesn't work for large files, or multiple files. Be smarter.
                                let readResult = stream.Read(buffer,0,int bufferLen)
                                readIOTimer.Dispose()
                                
                                if(readResult <> int bufferLen ) then
                                    raise (new Exception("wtf"))
                                if(buffer.[0] = byte 0) then
                                    raise (new Exception(sprintf "Read null data @ %A - %A\n%A" fileName requests buffer))    
                                // for every pointer requested to be read, merge them all into their own nodes
                                let outNodes = 
                                    requests
                                    |> Array.map(fun req ->
                                        let node = new Node()
                                        MessageExtensions.MergeFrom(node,buffer,int (req.Offset - requests.[0].Offset),int req.Length) 
                                        node
                                    )
                                
                                arraybuffer.Return(buffer, true) // todo: does this need to be in a finally block?
                                config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.ReadSize, tags, int64 bufferLen)
                                config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.ReadNodeFragmentCount, tags, int64 outNodes.Length)
                                tcs.SetResult(outNodes)
                                
                            else
                                tcs.SetResult(array.Empty<Node>())    
                            ReadTimer.Dispose()
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
                            
                            //let reply = IndexMaintainer.PostAndReply(Flush)
                            
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
                            
                            
                            //let reply = IndexMaintainer.PostAndReply(Flush)
                            
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
                                    let lst = new List<MemoryPointer>()
                                    lst.Add v
                                    FragmentLinksRequested.Add(k,lst)
                                else if FragmentLinksRequested.Item(k).Contains(v) = false then
                                    FragmentLinksRequested.Item(k) <- 
                                        let lst = FragmentLinksRequested.Item(k)
                                        lst.Add v
                                        lst 
                            
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
                    | NoOP(u) -> u
                                                                      
            finally
                config.log <| sprintf "Shutting down partition writer[%A]" i 
                config.log <| sprintf "Flushing partition writer[%A]" i
                FLUSHWRITES()
                stream.Dispose()
                (``Index of NodeID -> MemoryPointer`` :> IDisposable).Dispose()
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
