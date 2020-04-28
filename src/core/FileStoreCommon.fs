namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Generic
open System.Diagnostics
open System.Drawing
open System.IO
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc
open App.Metrics

type Config = {
    ParitionCount:int
    log: string -> unit
    CreateTestingDataDirectory:bool
    Metrics: IMetrics
    }

type IOStat = { readbytes: uint64; writebytes: uint64 }

type NodeIO =
    | Add of TaskCompletionSource<unit> * seq<Node>
    | Read  of TaskCompletionSource<array<Node>> * array<MemoryPointer>
    | FlushFixPointers of TaskCompletionSource<unit>
    | FlushAdds of TaskCompletionSource<unit>
    | FlushFragmentLinks of TaskCompletionSource<unit>
    | NoOP of unit

type IndexMessage =
    | Index of seq<Grpc.NodeID>
    | Flush of AsyncReplyChannel<bool>

[<System.FlagsAttribute>]
type WriteGroupsOperation = LinkFragments = 1 | FixPointers = 2  
    
type NodeIOGroup = { start:uint64; length:uint64; items:List<MemoryPointer> }

type IClusterServices =
    abstract RemoteLookup : int -> NodeID -> bool * MemoryPointer
