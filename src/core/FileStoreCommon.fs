namespace Ekati

open System
open System.Collections.Generic
open System.Diagnostics
open System.Drawing
open System.IO
open System.Threading
open System.Threading.Tasks
open Ekati.Core
open App.Metrics

type Config = {
    ParitionCount:int
    log: string -> unit
    CreateTestingDataDirectory:bool
    Metrics: IMetrics
    }

type IOStat = { readbytes: uint64; writebytes: uint64 }

type NodeIO =
    | Add of TaskCompletionSource<unit> * Node
    | Adds of TaskCompletionSource<unit> * seq<Node>
    | Read  of TaskCompletionSource<array<Node>> * array<NodeID>
    | FlushFixPointers of TaskCompletionSource<unit> * seq<Pointers>
    | NoOP of unit

type IndexMessage =
    | Index of seq<NodeID>
    | Flush of AsyncReplyChannel<bool>

[<System.FlagsAttribute>]
type WriteGroupsOperation = LinkFragments = 1 | FixPointers = 2  
    
type NodeIOGroup = { start:uint64; length:uint64; items:List<MemoryPointer> }

type IClusterServices =
    abstract RemoteLookup : int -> NodeID -> bool * Node
