namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
open System
open System.Collections.Generic
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
    | Write of TaskCompletionSource<unit> * Node
    | Read  of TaskCompletionSource<Node> * MemoryPointer
    | FlushFixPointers of TaskCompletionSource<unit>
    | FlushWrites of TaskCompletionSource<unit>

type IndexMessage =
    | Index of Grpc.AddressBlock * Grpc.MemoryPointer * TaskCompletionSource<unit> 
    | Flush of AsyncReplyChannel<bool>
    
type NodeIOGroup = { start:uint64; length:uint64; items:List<NodeID> }

type GrpcFileStore(config:Config) = 
    
    let ChooseMemoryPointerPartition (mp: MemoryPointer) =
        mp.Partitionkey
    
    let ChooseNodeIdHashPartition (nidHash: NodeIdHash) =
        let hash = uint32 <| nidHash.hash
        hash % (uint32 config.ParitionCount)
    
    let ChooseNodeIdPartition (nid: NodeID) = 
        let hash = uint32 <| nid.GetHashCode()
        hash % (uint32 config.ParitionCount)
        
    let rec ChoosePartition (ab:AddressBlock) =
        match ab.AddressCase with
        | AddressBlock.AddressOneofCase.Nodeid -> ChooseNodeIdPartition ab.Nodeid
        | AddressBlock.AddressOneofCase.Globalnodeid -> ChooseNodeIdPartition ab.Globalnodeid.Nodeid
        | AddressBlock.AddressOneofCase.None -> uint32 0  
        | _ -> uint32 0
                    
    
    
    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<NodeIdHash,Grpc.MemoryPointer>()
    
    let IndexMaintainer =
        MailboxProcessor<IndexMessage>.Start(fun inbox ->
            let rec messageLoop() = 
                async{
                    let! message = inbox.Receive()
                    match message with 
                    | Index(sn,mp, tcs) ->
                        try
                            let id = match sn.AddressCase with 
                                     | AddressBlock.AddressOneofCase.Globalnodeid -> sn.Globalnodeid.Nodeid
                                     | AddressBlock.AddressOneofCase.Nodeid -> sn.Nodeid
                                     | _ -> raise <| new NotImplementedException("AddressCase was not a NodeId or GlobalNodeID")
                            ``Index of NodeID -> MemoryPointer``.AddOrUpdate(Utils.GetNodeIdHash id, mp, (fun x y -> mp)) |> ignore
                            tcs.SetResult()                                                      
                        with
                        | ex -> tcs.SetException ex   
                    | Flush(replyChannel)->
                        replyChannel.Reply(true)
                        
                    return! messageLoop()
                }    
            messageLoop()    
        )

    let UpdateMemoryPointers (node:Node)=
        let AttachMemoryPointer (nodeid:NodeID) =
            if (nodeid.Pointer.Length = uint64 0) then
                let mutable outMp:MemoryPointer = Utils.NullMemoryPointer
                if (``Index of NodeID -> MemoryPointer``.TryGetValue (Utils.GetNodeIdHash nodeid, & outMp)) then
                    nodeid.Pointer <- outMp
                    true,true
                else
                    true,false 
            else 
                false,false    
    
        let updated = node.Attributes
                            |> Seq.collect (fun attr -> 
                                attr.Value
                                |> Seq.append [|attr.Key|])
                            |> Seq.map (fun tmd ->
                                            match  tmd.Data.DataCase with
                                            | DataBlock.DataOneofCase.Address -> 
                                                match tmd.Data.Address.AddressCase with 
                                                | AddressBlock.AddressOneofCase.Nodeid -> AttachMemoryPointer tmd.Data.Address.Nodeid
                                                | AddressBlock.AddressOneofCase.Globalnodeid -> AttachMemoryPointer tmd.Data.Address.Globalnodeid.Nodeid
                                                | _ ->  false,false
                                            | _ -> false,false)
        let anyChanged = 
            updated |> Seq.contains (true,true)
        let anyMissed =
            updated |> Seq.contains (true,false)                    
        anyChanged, anyMissed

    
    
    let BuildGroups (fpwb:SortedList<uint64,List<NodeID>>) = 
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
                length <- length + values.[0].Pointer.Length
                blockItems <- values    
            else // this **must** be the next contigious space in memory because the earlier if forces it
                length <- length + values.[0].Pointer.Length 
                blockItems.AddRange values
        
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
            // this function allows a thread to tell others how to add stuff to the collections
            // or to add stuff back into its own collections
            let fnNio (nio:NodeIO) = 
                let i,bc =
                    match nio with 
                    | Write(a,b) ->bcs.[int <| ChoosePartition (b.Ids |> Seq.head)]
                    | Read(a,b) -> bcs.[int <| ChooseMemoryPointerPartition b]
                    | x -> raise (new NotSupportedException(sprintf "Not Supported: %A" x))
                bc.Add (nio)
                
            let t = new ThreadStart((fun () -> 
                // TODO: If we cannot access this file, we need to mark this parition as offline, so it can be written to remotely
                // TODO: log file access failures
                
                let dir = match config.CreateTestingDataDirectory with 
                          | true -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("data-"+ Path.GetRandomFileName())))
                          | false -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data"))
                let fileNameid = i 
                let fileName = Path.Combine(dir.FullName, (sprintf "ahghee.%i.tmp" i))
                let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
                let out = new CodedOutputStream(stream)
                //let mutable buffer = Array.zeroCreate<byte> (1024)
                let FixPointersWriteBuffer = new SortedList<uint64,List<NodeID>>()
                let mutable lastOpIsWrite = false
                
                let FLUSHWRITES () =   
                    out.Flush()
                    stream.Flush()
                try
                    for nio in bc.GetConsumingEnumerable() do
                        match nio with
                        | Write(tcs,item) -> 
                            try
                                if (lastOpIsWrite = false) then
                                    stream.Seek (0L, IO.SeekOrigin.End) |> ignore
                                    lastOpIsWrite <- true
                                
                                let offset = out.Position
                                let mp = Ahghee.Grpc.MemoryPointer()
                                mp.Partitionkey <- uint32 i
                                mp.Filename <- uint32 fileNameid
                                mp.Offset <- uint64 offset
                                mp.Length <- (item.CalculateSize() |> uint64)
                                
                                let hid =(item.Ids |> Seq.head) 
                                let id = match hid.AddressCase with
                                            | AddressBlock.AddressOneofCase.Nodeid -> hid.Nodeid 
                                            | AddressBlock.AddressOneofCase.Globalnodeid -> hid.Globalnodeid.Nodeid
                                            | _ -> raise (new Exception("node id was not an address"))
                                // store the items own pointer in its address
                                id.Pointer <- mp    
                                
                                if (item.CalculateSize() |> uint64 <> mp.Length) then
                                    raise (new Exception(sprintf "Updating MemoryPointer changed Node Size - before: %A after: %A" mp.Length (item.CalculateSize() |> uint64)))
                                                                                
                                // update all NodeIds with their memoryPointers if we have them.
                                let (anyChanged,anyMissed) = UpdateMemoryPointers item
                                item.WriteTo out
                                
                                if (anyMissed) then
                                    if (FixPointersWriteBuffer.ContainsKey id.Pointer.Offset) then
                                        let l = (FixPointersWriteBuffer.Item id.Pointer.Offset)
                                        l.Add id
                                        let allOffsetsSame = l |> Seq.forall( fun x -> x.Pointer.Offset = id.Pointer.Offset )
                                        if(allOffsetsSame <> true) then
                                            tcs.SetException (new Exception("all offsets not same"))    
                                        ()    
                                    else
                                        FixPointersWriteBuffer.Add(id.Pointer.Offset,(new List<NodeID>([id])))
                                IndexMaintainer.Post (Index(hid, mp, tcs))
                                // TODO: Flush on interval, or other flush settings
                                //config.log <| sprintf "Flushing partition writer[%A]" i
                                //out.Flush()
                                //stream.Flush()
                                
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
                                raise (new Exception(sprintf "Read null data @ %A\n%A" request buffer))    
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
                                config.log (sprintf "####Flushing Index Maintainer for partion: %A" i)
                                let reply = IndexMaintainer.PostAndReply(Flush)
                                config.log (sprintf "####Flushed Index Maintainer for partition: %A = %A" i reply )
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
    
    member this.ChooseNodePartition (n:Node) =
        n.Ids
            |> Seq.map (fun x -> ChoosePartition x) 
            |> Seq.head
                    
    interface IStorage with
        member x.Nodes = 
            // return local nodes before remote nodes
            // let just start by pulling nodes from the index.
            
            // todo: this could be a lot smarter and fetch from more than one partition reader at a time
            // todo: additionally, Using the index likely results in Random file access, we could instead
            // todo: just read from the file sequentially
            seq { for kv in ``Index of NodeID -> MemoryPointer`` do
                  let tcs = new TaskCompletionSource<Node>()
                  let (bc,t) = PartitionWriters.[int <| ChooseNodeIdHashPartition kv.Key]
                  bc.Add (Read( tcs, kv.Value)) 
                  yield tcs.Task.Result
                }
            // todo return remote nodes
            
        member x.Flush () = Flush()
            
        member this.Add (nodes:seq<Node>) = 
            Task.Factory.StartNew(fun () -> 
                for (n) in nodes do
                    let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)         
                    let (bc,t) = PartitionWriters.[int <| this.ChooseNodePartition n]
                    bc.Add (Write(tcs,n))
                )
                        
        member x.Remove (nodes:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<AddressBlock>) = 
            let requestsMade =
                addressBlock
                |> Seq.map (fun ab ->
                    let tcs = new TaskCompletionSource<Node>()
                    let (bc,t) = PartitionWriters.[int <| ChoosePartition ab]
                    let nid = match ab.AddressCase with 
                              | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid
                              | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid
                              | _ -> raise (new ArgumentException("AddressBlock did not contain a valid AddressCase"))
                    
                    let t = 
                        if (nid.Pointer = Utils.NullMemoryPointer) then
                            let mutable mp = Utils.NullMemoryPointer
                            if(``Index of NodeID -> MemoryPointer``.TryGetValue(Utils.GetNodeIdHash nid, &mp)) then 
                                bc.Add (Read(tcs, mp))
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
