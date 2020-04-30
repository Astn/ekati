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
    interface Ahghee.Grpc.IStorage with
        member x.Flush () = ()
        member this.Add (nodes:seq<Node>) = 
            _nodes <- Seq.append _nodes nodes
            Task.CompletedTask
        member this.Remove (nodeIDs: seq<NodeID>) = 
            _nodes <- _nodes |> Seq.filter (fun n -> 
                                                    let head = n.Id 
                                                    nodeIDs |> Seq.contains head |> not)
            Task.CompletedTask    
        
        member this.Nodes () =
            _nodes
            
        member this.Items (addresses:seq<NodeID>, follow: Step) =
            let matches = addresses |> Seq.map (fun addr -> 
                                                        let isLocal = _nodes 
                                                                      |> Seq.tryFind ( fun n -> n.Id = addr)
                                                        let found = match isLocal with
                                                            | Some(n) -> Either<Node,Exception>(n)
                                                            | _ -> Either<Node,Exception>(new Exception("Not Found"))
                                                        struct (addr, found)
                                                                
                                                )
            Task.FromResult matches      
        member this.First (predicate: Func<Node,bool>) : System.Threading.Tasks.Task<Node> =
            _nodes
            |> Seq.tryFind (fun x -> predicate.Invoke(x))  
            |> (fun x ->
                  if x.IsSome then Task.FromResult(x.Value) else Task.FromResult(null)  
                )  
        member this.Stop() = ()
        
        member x.GetStats(req, cancel) =
            raise (new NotImplementedException())
        
        member x.GetMetrics(req, cancel) =
            raise (new NotImplementedException())
