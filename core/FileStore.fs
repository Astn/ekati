namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
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

type IndexMessage =
    | Index of Grpc.NodeID
    | Flush of AsyncReplyChannel<bool>
    
type NodeIOGroup = { start:uint64; length:uint64; items:List<NodeID> }

type GrpcFileStore(config:Config) = 

    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<NodeIdHash,seq<Grpc.MemoryPointer>>()
    
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
    

    let LinkFragments (n:Node) =
        let hash = n.Id |> NodeIdFromAddress |> Utils.GetNodeIdHash
        let mutable outMp:seq<MemoryPointer> = Seq.empty<MemoryPointer>
        if (``Index of NodeID -> MemoryPointer``.TryGetValue (hash, & outMp)) then
            for mp in outMp do
                if (mp.Partitionkey <> n.Id.Nodeid.Pointer.Partitionkey
                    && mp.Filename <> n.Id.Nodeid.Pointer.Filename
                    && mp.Offset <> n.Id.Nodeid.Pointer.Offset) then 
                    // Assign other addresses to this node, but maybe not all of them.
                    // We don't use ADD because that would change the memory size for this fragment.
                    // Instead we find a reserved null pointer and update it
                    for i in 0 .. n.Fragments.Count do
                        if n.Fragments.Item(0) = Utils.NullMemoryPointer() then 
                            n.Fragments.Item(0) <- mp
                    // TODO: Also, we may need to tell other fragments about this one
                    // TODO: If there are more than a few fragments, we need to link them In a way where they are all connected
                    ()

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


        LinkFragments node
        
             
        let updated = node.Attributes
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

    
    
    let BuildGroups (fpwb:SortedList<uint64,NodeID>) = 
        let groups = new List<NodeIOGroup>()
        let mutable blockStarted = false
        let mutable start =uint64 0
        let mutable length = uint64 0
        let mutable blockItems = new List<NodeID>()
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
               blockItems <- new List<NodeID>() 
        
            if (blockStarted = false) then
                blockStarted <- true 
                start <- key
                length <- length + values.Pointer.Length
                blockItems.Add values    
            else // this **must** be the next contigious space in memory because the earlier if forces it
                length <- length + values.Pointer.Length 
                blockItems.Add values
        
        // close out last group
        if (blockItems.Count > 0) then
            groups.Add { start=start;length=length;items=blockItems }
        
        groups    

    // returns true if we flushed or moved the position in the out or filestream
    let writeGroups groups ofSize (stream:FileStream) = 
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
                            | _ when int64 nid.Pointer.Offset = stream.Position -> 
                                lastex <- null
                                let toFix = new Node()
                                let buffer = Array.zeroCreate<byte> (int nid.Pointer.Length)
    
                                let readResult = stream.Read(buffer,0,int nid.Pointer.Length)
                                if(readResult <> int nid.Pointer.Length || buffer.[0] = byte 0) then
                                    raise (new Exception("wtf"))
                                MessageExtensions.MergeFrom(toFix,buffer,0,int nid.Pointer.Length)
                                // now fix it.
                                let (anyChanged,anyMissed) = UpdateMemoryPointers toFix 
                                
                                if (anyChanged && toFix.CalculateSize() |> uint64 <> nid.Pointer.Length) then
                                    raise (new Exception(sprintf "Updating MemoryPointer changed Node Size - before: %A after: %A" nid.Pointer.Length (toFix.CalculateSize() |> uint64)))
                                
                                if (anyChanged) then    
                                    // !!!!do the writing later in a batch..
                                    writeback.Add (nid.Pointer, toFix)
                            | _ when int64 nid.Pointer.Offset < stream.Position -> 
                                // this should be because its a duplicate request for the previous item
                                ()
                            | _ when int64 nid.Pointer.Offset > stream.Position ->
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
                let FixPointersWriteBuffer = new SortedList<uint64,NodeID>()
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
                                        FixPointersWriteBuffer.Add(id.Pointer.Offset,id)
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
                                if( writeGroups groups anySize stream ) then
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
        let allDone =
            seq {for (bc,t) in PartitionWriters do
                    let fwtcs = new TaskCompletionSource<unit>()
                    bc.Add( FlushWrites(fwtcs))
                    let tcs = new TaskCompletionSource<unit>()
                    bc.Add( FlushFixPointers(tcs))
                    yield [ fwtcs.Task :> Task; tcs.Task :> Task]}
            |> Seq.collect (fun x -> x)
            |> Array.ofSeq    
            |> Task.WhenAll
        if (allDone.IsFaulted) then
            raise allDone.Exception
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
                      bc.Add (Read( tcs, (kv.Value |> Seq.head))) 
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
