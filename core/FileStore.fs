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

type GrpcFileStore(config:Config) = 

    let rec ChoosePartition (ab:AddressBlock) =
        let hash = match ab.AddressCase with
                    | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid.Graph.GetHashCode() * 31 + ab.Nodeid.Nodeid.GetHashCode()
                    | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid.Graph.GetHashCode() * 31 + ab.Globalnodeid.Nodeid.Nodeid.GetHashCode()
                    | AddressBlock.AddressOneofCase.None -> 0  
                    | _ -> 0
        Math.Abs(hash) % config.ParitionCount                    
    
    // TODO: Switch to PebblesDB when index gets to big
    let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.NodeID,Grpc.MemoryPointer>()
    let ``Index of NodeID without MemoryPointer -> NodeId that need them`` = new System.Collections.Concurrent.ConcurrentDictionary<Grpc.NodeID,list<Grpc.NodeID>>()
    
    let IndexMaintainer =
        MailboxProcessor<Grpc.NodeID * Grpc.MemoryPointer >.Start(fun inbox ->
            let rec messageLoop() = 
                async{
                    let! (sn,mp) = inbox.Receive()
                    ``Index of NodeID -> MemoryPointer``.AddOrUpdate(sn, mp, (fun x y -> mp)) |> ignore
                    return! messageLoop()
                }
            messageLoop()    
        )
        
    let PartitionWriters = 
        seq {0 .. (config.ParitionCount - 1)}
        |>  Seq.map (fun i -> 
            let bc = new System.Collections.Concurrent.BlockingCollection<TaskCompletionSource<unit> * Node>()
            let t = new ThreadStart((fun () -> 
                // TODO: If we cannot access this file, we need to mark this parition as offline, so it can be written to remotely
                // TODO: log file access failures
                
                let dir = IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data-"+config.DataDirectoryPostfix))
                
                let fileName = Path.Combine(dir.FullName, (sprintf "ahghee.%i.tmp" i))
                let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024,IO.FileOptions.Asynchronous ||| IO.FileOptions.RandomAccess)
                let posEnd = stream.Seek (0L, IO.SeekOrigin.End)
                let out = new CodedOutputStream(stream)
                try
                    for (tcs,item) in bc.GetConsumingEnumerable() do 
                        try
                            let offset = out.Position
                            let mp = Ahghee.Grpc.MemoryPointer()
                            mp.Partitionkey <- i.ToString() 
                            mp.Filename <- fileName
                            mp.Offset <- offset
                            mp.Length <- (item.CalculateSize() |> int64)
                            item.WriteTo out
                            //config.log <| sprintf "Finished[%A]: %A" i item
                            config.log <| sprintf "TaskStatus-1: %A" tcs.Task.Status
                            tcs.SetResult(())
                            config.log <| sprintf "TaskStatus-2: %A" tcs.Task.Status
                            // TODO: Flush on interval, or other flush settings
                            config.log <| sprintf "Flushing partition writer[%A]" i
                            out.Flush()
                            stream.Flush()
                        with 
                        | :? Exception as ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                finally
                    config.log <| sprintf "Shutting down partition writer[%A]" i 
                    config.log <| sprintf "Flushing partition writer[%A]" i
                    out.Flush()
                    stream.Flush()
                    out.Dispose()
                    stream.Close()
                    stream.Dispose()
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
        member x.Nodes = raise (new NotImplementedException())
        member x.Flush () = 
            ()
            
        member this.Add (nodes:seq<Node>) = 
            Task.Factory.StartNew(fun () -> 
                for (n) in nodes do
                    let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.AttachedToParent)         
                    let (bc,t) = PartitionWriters.[this.ChooseNodePartition n]
                    bc.Add ((tcs,n))
                )
                        
        member x.Remove (nodes:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.Items (addressBlock:seq<AddressBlock>) = raise (new NotImplementedException())
        member x.First (predicate: (Node -> bool)) = raise (new NotImplementedException())
        member x.Stop () =  for (bc,t) in PartitionWriters do
                                bc.CompleteAdding()
                                t.Join()                
 