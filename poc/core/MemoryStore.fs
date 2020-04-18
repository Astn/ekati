namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Generic
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
        member this.Remove (nodeIDs:seq<NodeID>) = 
            _nodes <- _nodes |> Seq.filter (fun n -> 
                                                    let head = n.Id 
                                                    nodeIDs |> Seq.contains head |> not)
            Task.CompletedTask    
        member this.Items (addresses:seq<NodeID>, follow: IDictionary<DataBlock, int>) =
            let matches = addresses |> Seq.map (fun addr -> 
                                                        let isLocal = _nodes 
                                                                      |> Seq.tryFind ( fun n -> n.Id = addr)
                                                        let found = match isLocal with
                                                            | Some(n) -> Left(n)
                                                            | _ -> Right(new Exception("Not Found"))
                                                        (addr, found)
                                                                
                                                )
            Task.FromResult matches      
        member this.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> =
            _nodes
            |> Seq.tryFind predicate  
            |> Task.FromResult 
        member this.Stop() = ()                                              
