namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc

type MemoryStore() =
    let mutable _nodes:seq<Node> = Seq.empty
    interface IStorage with
        member this.Nodes = _nodes
        member x.Flush () = ()
        member this.Add (nodes:seq<Node>) = 
            _nodes <- Seq.append _nodes nodes
            Task.CompletedTask
        member this.Remove (nodeIDs:seq<AddressBlock>) = 
            _nodes <- _nodes |> Seq.filter (fun n -> 
                                                    let head = n.Id 
                                                    nodeIDs |> Seq.contains head |> not)
            Task.CompletedTask    
        member this.Items (addresses:seq<AddressBlock>) =
            let matches = addresses |> Seq.map (fun addr -> 
                                                match addr.AddressCase with 
                                                | AddressBlock.AddressOneofCase.Nodeid -> 
                                                                let isLocal = _nodes 
                                                                              |> Seq.tryFind ( fun n -> n.Id = addr)
                                                                match isLocal with 
                                                                | Some node -> (addr, Left(node))
                                                                | None -> (addr, Right (Failure "remote nodes not supported yet"))
                                                | AddressBlock.AddressOneofCase.Globalnodeid -> raise (new NotImplementedException())
                                                | AddressBlock.AddressOneofCase.None -> raise (new NotImplementedException())
                                                | _ -> raise (new NotImplementedException())
                                                )
            Task.FromResult matches      
        member this.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> =
            _nodes
            |> Seq.tryFind predicate  
            |> Task.FromResult 
        member this.Stop() = ()                                              
