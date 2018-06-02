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

type Config = {
    ParitionCount:int
    log: string -> unit
    CreateTestingDataDirectory:bool
    }


type NodeIO =
    | Write of TaskCompletionSource<unit> * seq<Node>
    | Read  of TaskCompletionSource<Node> * MemoryPointer
    | FlushFixPointers of TaskCompletionSource<unit>
    | FlushWrites of TaskCompletionSource<unit>
    | FlushFragmentLinks of TaskCompletionSource<unit>

type IndexMessage =
    | Index of Grpc.NodeID
    | Flush of AsyncReplyChannel<bool>

[<System.FlagsAttribute>]
type WriteGroupsOperation = LinkFragments = 1 | FixPointers = 2  
    
type NodeIOGroup = { start:uint64; length:uint64; items:List<MemoryPointer> }

type GrpcFileStore(config:Config) = 

    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<NodeIdHash, seq<Grpc.MemoryPointer>>()
    
    // TODO: These next two indexes may be able to be specific to a particular thread writer, and then they wouldn't need to be concurrent if that is the case.
    // The idea behind this index is to know which connections we do not need to make as they are already made
    let FragmentLinksConnected = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.MemoryPointer, seq<Grpc.MemoryPointer>>()
    // The idea behind this index is to know which connections we know we need to make that we have not yet made
    let FragmentLinksRequested = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.MemoryPointer, seq<Grpc.MemoryPointer>>()
    
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

        
    let NodeIdFromAddress (addr:AddressBlock) =
        match addr.AddressCase with 
        | AddressBlock.AddressOneofCase.Nodeid -> addr.Nodeid
        | AddressBlock.AddressOneofCase.Globalnodeid -> addr.Nodeid
        | _ ->  raise (new NotSupportedException("AddresBlock did not contain a valid address"))
    
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
                        FragmentLinksConnected.AddOrUpdate (n.Id.Nodeid.Pointer, [mp], (fun key value -> value |> Seq.append [mp] ))
                        |> ignore
                        let mutable existingLinks = Seq.empty<MemoryPointer>
                        
                        // If this was a requested link, remove it from the requests
                        if FragmentLinksRequested.TryGetValue(n.Id.Nodeid.Pointer, & existingLinks) && existingLinks |> Seq.contains mp then 
                            if existingLinks |> Seq.length > 1 then 
                                FragmentLinksRequested.TryUpdate (n.Id.Nodeid.Pointer, existingLinks |> Seq.except [mp], existingLinks)
                            else
                                FragmentLinksRequested.TryRemove(n.Id.Nodeid.Pointer, & existingLinks)                                        
                            |> ignore
                            
                        // Track that we want to link it from the other direction, but only if that hasn't already been done
                        if FragmentLinksConnected.TryGetValue(mp, & existingLinks) = false || existingLinks |> Seq.contains n.Id.Nodeid.Pointer = false then 
                            FragmentLinksRequested.AddOrUpdate (mp, [n.Id.Nodeid.Pointer], (fun key value -> value |> Seq.append [n.Id.Nodeid.Pointer] ))
                            |> ignore

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
                let mutable outMp:seq<MemoryPointer> = Seq.empty<MemoryPointer>
                if (``Index of NodeID -> MemoryPointer``.TryGetValue (Utils.GetNodeIdHash nodeid, & outMp)) then
                    nodeid.Pointer <- outMp |> Seq.head
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
    let writeGroups groups ofSize (stream:FileStream) (op:WriteGroupsOperation)= 
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
                            stream.Flush() 
                           
                        true
                    else 
                        false}
        |> Seq.contains true            
                    
   
    let PartitionWriters = 
        let bcs = 
            seq {for i in 0 .. (config.ParitionCount - 1) do 
                 yield i, new System.Collections.Concurrent.BlockingCollection<NodeIO>()}
            |> Array.ofSeq
        bcs    
        |>  Seq.map (fun (i,bc) -> 
                
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
                    // TODO: ?? maybe only flush if out.pos > stream.pos ?? Flushing may make Out no longer usable, and further flushes might corupt the underlying stream?
                    if (out.Position > stream.Position) then
                        out.Flush()
                        stream.Flush()
                try
                    for nio in bc.GetConsumingEnumerable() do
                        match nio with
                        | Write(tcs,items) -> 
                            try
                                
                                let FixPointersWriteBufferTime = new Stopwatch()
                                let CreatePointerTime = new Stopwatch()
                                let WriteTime = new Stopwatch()
                                let IndexMaintainerPostTime = new Stopwatch()
                                let StopWatchTime = Stopwatch.StartNew()
                                if (lastOpIsWrite = false) then
                                    stream.Position <- lastPosition
                                    lastOpIsWrite <- true
                                let startPos = out.Position
                                for item in items do
                                    
                                    CreatePointerTime.Start()
                                    let offset = out.Position
                                    let id = item.Id.Nodeid
                                    let mp = id.Pointer
                                    mp.Partitionkey <- uint32 i
                                    mp.Filename <- uint32 fileNameid
                                    mp.Offset <- uint64 offset
                                    mp.Length <- (item.CalculateSize() |> uint64)

                                    CreatePointerTime.Stop()

                                    WriteTime.Start()
                                    item.WriteTo out
                                    WriteTime.Stop()
                                    FixPointersWriteBufferTime.Start()
                                    if (not (FixPointersWriteBuffer.ContainsKey id.Pointer.Offset)) then
                                        FixPointersWriteBuffer.Add(id.Pointer.Offset,id.Pointer)
                                    else
                                        ()    
                                    FixPointersWriteBufferTime.Stop()
                                    IndexMaintainerPostTime.Start()                                                                                
                                    IndexMaintainer.Post (Index(id))
                                    IndexMaintainerPostTime.Stop()

                                lastPosition <- out.Position
                                StopWatchTime.Stop()
                                let swtime = StopWatchTime.Elapsed -  CreatePointerTime.Elapsed -  FixPointersWriteBufferTime.Elapsed - WriteTime.Elapsed - IndexMaintainerPostTime.Elapsed      
                                config.log <| sprintf "--->\nCreatePointerTime: %A\nFixPointersWriteBufferTime: %A\nWriteTime: %A\nIndexMaintainerPostTime: %A\nStopWatchTime: %A\nTotalTime:%A\nbytesWritten:%A\n<---" CreatePointerTime.Elapsed FixPointersWriteBufferTime.Elapsed WriteTime.Elapsed IndexMaintainerPostTime.Elapsed swtime StopWatchTime.Elapsed (lastPosition - startPos)   
                                tcs.SetResult()
                            with 
                            | ex -> 
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)
                        | Read (tcs,request) ->  
                            FLUSHWRITES()                          
                            lastOpIsWrite <- false
                            stream.Position <- int64 request.Offset
                            
                            let nnnnnnnn = new Node()
                            let buffer = Array.zeroCreate<byte> (int request.Length)
//                                if (buffer.Length < (int request.Length)) then
//                                    Array.Resize ((ref buffer), (int request.Length))
                            
                            let readResult = stream.Read(buffer,0,int request.Length)
                            if(readResult <> int request.Length ) then
                                raise (new Exception("wtf"))
                            if(buffer.[0] = byte 0) then
                                raise (new Exception(sprintf "Read null data @ %A - %A\n%A" fileName request buffer))    
                            try     
                                MessageExtensions.MergeFrom(nnnnnnnn,buffer,0,int request.Length)
                                
                                tcs.SetResult(nnnnnnnn)
                            with 
                            | ex -> 
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)
                        | FlushFixPointers (tcs) ->
                            try
                                FLUSHWRITES()    
                                let groups = BuildGroups FixPointersWriteBuffer        
                                FixPointersWriteBuffer.Clear()
                                //config.log (sprintf "####Flushing Index Maintainer for partion: %A" i)
                                let reply = IndexMaintainer.PostAndReply(Flush)
                                //config.log (sprintf "####Flushed Index Maintainer for partition: %A = %A" i reply )
                                let anySize = float 0
                                if( writeGroups groups anySize stream (WriteGroupsOperation.FixPointers ||| WriteGroupsOperation.LinkFragments)) then
                                    lastOpIsWrite <- false
                                tcs.SetResult()  
                            with 
                            | ex -> 
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)        
                        | FlushWrites (tcs) ->
                            try 
                                FLUSHWRITES()
                                tcs.SetResult()
                            with 
                            | ex ->
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)      
                        | FlushFragmentLinks (tcs) ->
                            try 
                                FLUSHWRITES()
                                // And read, and update each of those fragments calling LinkFragments.
                                
                                
                                let reply = IndexMaintainer.PostAndReply(Flush)
                                
                                // we may have to do this multiple times as the FragmentLinksRequested my get new entries
                                // as we satisify the ones in there. 
                                // Todo: We might be able to avoid that by pre processing the
                                // FragmentLinksRequested to also contain the bidirectional link for anything in it. 
                                let mutable doAgain = true
                                while doAgain do 
                                    // buildgroups from the keys of FragmentLinksRequested that are for this partition.
                                    let groupsInput = new SortedList<uint64,MemoryPointer>() 
                                    for mp in (FragmentLinksRequested.Keys 
                                                |> Seq.filter (fun x -> x.Partitionkey = uint32 i)
                                                |> Seq.sortBy (fun x -> x.Offset)) do
                                        groupsInput.Add(mp.Offset, mp)
                                    let groups = BuildGroups groupsInput    
                                    
                                    let anySize = float 0
                                    if( writeGroups groups anySize stream WriteGroupsOperation.LinkFragments) then
                                        lastOpIsWrite <- false
                                    doAgain <-     
                                        FragmentLinksRequested.Keys 
                                            |> Seq.filter (fun x -> x.Partitionkey = uint32 i)
                                            |> Seq.exists (fun x -> true)
                                    
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
            (bc, thread)
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
