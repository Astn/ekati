namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc

type Config = {
    ParitionCount:int
    log: string -> unit
    DataDirectoryPostfix:string
    }


type NodeIO =
    | Write of TaskCompletionSource<unit> * Node
    | Read  of TaskCompletionSource<Node> * MemoryPointer

type GrpcFileStore(config:Config) = 
    let hasher = System.Data.HashFunction.MurmurHash.MurmurHash3Factory.Instance.Create()
    let ChooseNodeIdPartition (nid: NodeID) = 
        let sourceG = System.Text.Encoding.UTF8.GetBytes(nid.Graph)
        let sourceN = System.Text.Encoding.UTF8.GetBytes(nid.Nodeid)
        let array = Array.zeroCreate<byte> (sourceG.Length + sourceN.Length)    
        Array.Copy(sourceG, 0, array, 0, sourceG.Length)
        Array.Copy(sourceN, 0, array, sourceG.Length, sourceN.Length) 
        let hash = hasher.ComputeHash(array)
        BitConverter.ToUInt32( hash.Hash, 0) % (uint32 config.ParitionCount)
        
    let rec ChoosePartition (ab:AddressBlock) =
        match ab.AddressCase with
        | AddressBlock.AddressOneofCase.Nodeid -> ChooseNodeIdPartition ab.Nodeid
        | AddressBlock.AddressOneofCase.Globalnodeid -> ChooseNodeIdPartition ab.Globalnodeid.Nodeid
        | AddressBlock.AddressOneofCase.None -> uint32 0  
        | _ -> uint32 0
                    
    
    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.NodeID,Grpc.MemoryPointer>()
    let ``Index of NodeID without MemoryPointer -> NodeId that need them`` = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.NodeID,list<Grpc.NodeID>>()
    
    let IndexMaintainer =
        MailboxProcessor<Grpc.AddressBlock * Grpc.MemoryPointer >.Start(fun inbox ->
            let rec messageLoop() = 
                async{
                    let! (sn,mp) = inbox.Receive()
                    let id = match sn.AddressCase with 
                             | AddressBlock.AddressOneofCase.Globalnodeid -> sn.Globalnodeid.Nodeid
                             | AddressBlock.AddressOneofCase.Nodeid -> sn.Nodeid
                             | _ -> raise <| new NotImplementedException("AddressCase was not a NodeId or GlobalNodeID")
                    
                    ``Index of NodeID -> MemoryPointer``.AddOrUpdate(id, mp, (fun x y -> mp)) |> ignore
                    return! messageLoop()
                }
            messageLoop()    
        )

        
    let PartitionWriters = 
        seq {0 .. (config.ParitionCount - 1)}
        |>  Seq.map (fun i -> 
            let bc = new System.Collections.Concurrent.BlockingCollection<NodeIO>()
            let t = new ThreadStart((fun () -> 
                // TODO: If we cannot access this file, we need to mark this parition as offline, so it can be written to remotely
                // TODO: log file access failures
                
                let dir = IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data-"+config.DataDirectoryPostfix))
                
                let fileName = Path.Combine(dir.FullName, (sprintf "ahghee.%i.tmp" i))
                let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
                let out = new CodedOutputStream(stream)
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
                                mp.Partitionkey <- i.ToString() 
                                mp.Filename <- fileName
                                mp.Offset <- offset
                                mp.Length <- (item.CalculateSize() |> int64)
                                item.WriteTo out
                               
                                IndexMaintainer.Post (item.Ids |> Seq.head, mp)
                                // TODO: Flush on interval, or other flush settings
                                config.log <| sprintf "Flushing partition writer[%A]" i
                                out.Flush()
                                stream.Flush()
                                tcs.SetResult(())
                            with 
                            | ex -> 
                                config.log <| sprintf "ERROR[%A]: %A" i ex
                                tcs.SetException(ex)
                        | Read (tcs,request) ->  
                            try                            
                                lastOpIsWrite <- false
                                stream.Seek(request.Offset,SeekOrigin.Begin) |> ignore
                                
                                let nnnnnnnn = new Node()
                                let buffer = Array.zeroCreate<byte> (int request.Length)
                                stream.Read(buffer,0,int request.Length) |> ignore
                                MessageExtensions.MergeFrom(nnnnnnnn,buffer)
                                
                                tcs.SetResult(nnnnnnnn)
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
                  let (bc,t) = PartitionWriters.[int <| ChooseNodeIdPartition kv.Key]
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
        member x.Items (addressBlock:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.First (predicate: (Node -> bool)) = raise (new NotImplementedException())
        member x.Stop () =  for (bc,t) in PartitionWriters do
                                bc.CompleteAdding()
                                t.Join()    
                            while IndexMaintainer.CurrentQueueLength > 0 do 
                                Thread.Sleep(10)            
 