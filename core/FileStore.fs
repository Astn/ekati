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
    | FixPointers of TaskCompletionSource<unit> * MemoryPointer

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
    
    // TODO: a value of a SortedList might have better performance when we get a lot of items. Test it.
    let Index2 = new System.Collections.Concurrent.ConcurrentDictionary<NodeIdHash,Set<Grpc.NodeID>>()

    let UpdateMemoryPointers (node:Node)=
        let UpdateForNodeId (nodeid:NodeID) =
            if (nodeid.Pointer.Length = uint64 0) then
                let mutable outMp:MemoryPointer = Utils.NullMemoryPointer
                if (``Index of NodeID -> MemoryPointer``.TryGetValue (Utils.GetNodeIdHash nodeid, & outMp)) then
                    nodeid.Pointer <- outMp
                    true
                else
                    nodeid.Pointer <- Utils.NullMemoryPointer
                    let needsIt = (node.Ids |> Seq.head).Nodeid
                    Index2.AddOrUpdate(
                        Utils.GetNodeIdHash nodeid, 
                        new Set<NodeID>( [|needsIt|]) , 
                        (fun (nid:NodeIdHash) (s:Set<NodeID>) -> s.Add needsIt) ) |> ignore
                    false    
            else 
                false    
    
        let updated = node.Attributes
                            |> Seq.collect (fun attr -> attr.Value)
                            |> Seq.map (fun tmd ->
                                            match  tmd.Data.DataCase with
                                            | DataBlock.DataOneofCase.Address -> 
                                                match tmd.Data.Address.AddressCase with 
                                                | AddressBlock.AddressOneofCase.Nodeid -> UpdateForNodeId tmd.Data.Address.Nodeid
                                                | AddressBlock.AddressOneofCase.Globalnodeid -> UpdateForNodeId tmd.Data.Address.Globalnodeid.Nodeid
                                                | _ ->  false
                                            | _ -> false)
                            |> Seq.contains true                
        updated

    
    let IndexMaintainer =
        MailboxProcessor<Grpc.AddressBlock * Grpc.MemoryPointer * (NodeIO -> unit) * TaskCompletionSource<unit> >.Start(fun inbox ->
            let rec messageLoop() = 
                async{
                    let! (sn,mp, fnNodeIO, tcs) = inbox.Receive()
                    try
                        let id = match sn.AddressCase with 
                                 | AddressBlock.AddressOneofCase.Globalnodeid -> sn.Globalnodeid.Nodeid
                                 | AddressBlock.AddressOneofCase.Nodeid -> sn.Nodeid
                                 | _ -> raise <| new NotImplementedException("AddressCase was not a NodeId or GlobalNodeID")
                        
                        ``Index of NodeID -> MemoryPointer``.AddOrUpdate(Utils.GetNodeIdHash id, mp, (fun x y -> mp)) |> ignore
                        // check if there are any nodes looking for the one we just added.
                        let mutable fixers = Set.empty<NodeID>
                        // TODO: Why are we not getting into this If block..!!??
                        //config.log (sprintf "Index2 length: %A - Contains Item: %A" Index2.Count (Index2.ContainsKey (Utils.GetNodeIdHash id)))
                        // TODO: TryGetValue doesn't every remove anything.
                        // We need to use TryRemove, but there is a race condition I'm thinking.
                        if (Index2.TryRemove((Utils.GetNodeIdHash id), & fixers)) then
                            let sorted = fixers 
                                         |> Seq.sortBy (fun x -> int <| x.Pointer.Offset)
                                                                                                 
                            let allDone = seq {for f in sorted do
                                                let tcs = new TaskCompletionSource<unit>()
                                                // Make sure to ask for data out of the correct reader!!!
                                                fnNodeIO (FixPointers (tcs, f.Pointer))
                                                yield tcs.Task
                                                }
                                            |> Task.WhenAll
                            allDone.ContinueWith((fun (t:Task) -> 
                                    if (t.IsCompletedSuccessfully) then 
                                        tcs.SetResult() |> ignore
                                    else
                                        tcs.SetException(t.Exception) |> ignore
                                )) |> ignore  
                        else 
                            //config.log (sprintf "Index2 Missing: %A \n Contains Items: \n%A" (Utils.GetNodeIdHash id) Index2.Keys)  
                            tcs.SetResult()                                                      
                    with
                    | ex -> tcs.SetException ex   
                        
                    return! messageLoop()
                }    
            messageLoop()    
        )
    
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
                    | FixPointers(a,b) -> bcs.[int <| ChooseMemoryPointerPartition b]
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
                
                let mutable lastOpIsWrite = false
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
                                UpdateMemoryPointers item |> ignore
                                item.WriteTo out
                                //out.Flush()  
                                //stream.Flush()
                                IndexMaintainer.Post (item.Ids |> Seq.head, mp, fnNio, tcs)
                                // TODO: Flush on interval, or other flush settings
                                //config.log <| sprintf "Flushing partition writer[%A]" i
                                //out.Flush()
                                //stream.Flush()
                                
                            with 
                            | ex -> 
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)
                        | Read (tcs,request) ->  
                            try 
                                // gotta flush before we try to read data we put in it.
                                // possibly can avoid flush if the offset we want is prior to our last flush.
                                // but this might not work if we don't flush after we do a FixPointers.
                                out.Flush()
                                stream.Flush()                           
                                lastOpIsWrite <- false
                                stream.Position <- int64 request.Offset
                                
                                let nnnnnnnn = new Node()
                                let buffer = Array.zeroCreate<byte> (int request.Length)
//                                if (buffer.Length < (int request.Length)) then
//                                    Array.Resize ((ref buffer), (int request.Length))
                                
                                stream.Read(buffer,0,int request.Length) |> ignore
                                MessageExtensions.MergeFrom(nnnnnnnn,buffer,0,int request.Length)
                                
                                tcs.SetResult(nnnnnnnn)
                            with 
                            | ex -> 
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)
                        | FixPointers (tcs,request) ->
                            try    
                                // gotta flush before we try to read data we put in it.
                                out.Flush()  
                                stream.Flush()                      
                                lastOpIsWrite <- false
                                stream.Position <- int64 request.Offset
                                
                                let toFix = new Node()
                                let buffer = Array.zeroCreate<byte> (int request.Length)
//                                if (buffer.Length < (int request.Length)) then
//                                    Array.Resize ((ref buffer), (int request.Length))
                                let readResult = stream.Read(buffer,0,int request.Length)
                                if(readResult <> int request.Length || buffer.[0] = byte 0) then
                                    raise (new Exception("wtf"))
                                MessageExtensions.MergeFrom(toFix,buffer,0,int request.Length)
                                // now fix it.
                                if (UpdateMemoryPointers toFix ) then
                                    if (toFix.CalculateSize() |> uint64 <> request.Length) then
                                        raise (new Exception(sprintf "Updating MemoryPointer changed Node Size - before: %A after: %A" request.Length (toFix.CalculateSize() |> uint64)))
                                    stream.Seek(int64 request.Offset,SeekOrigin.Begin) |> ignore                   
                                    toFix.WriteTo out
                                
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
            
        member x.Flush () = 
            ()
            
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
        member x.Stop () =  for (bc,t) in PartitionWriters do
                                bc.CompleteAdding()
                                t.Join()    
                            while IndexMaintainer.CurrentQueueLength > 0 do 
                                Thread.Sleep(10)            
 