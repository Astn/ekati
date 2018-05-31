namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
open System
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc

type Either<'L, 'R> =
    | Left of 'L
    | Right of 'R

type NodeIdHash = { hash:int; graph:string; nodeid:string }

type IStorage =
    abstract member Nodes: seq<Node>
    abstract member Flush: unit -> unit
    abstract member Add: seq<Node> -> System.Threading.Tasks.Task
    abstract member Remove: seq<AddressBlock> -> System.Threading.Tasks.Task
    abstract member Items: seq<AddressBlock> -> System.Threading.Tasks.Task<seq<AddressBlock * Either<Node, Exception>>>
    abstract member First: (Node -> bool) -> System.Threading.Tasks.Task<Option<Node>> 
    abstract member Stop: unit -> unit


type Graph(storage:IStorage) =  
    member x.Nodes = storage.Nodes
    member x.Flush () = storage.Flush()
    member x.Add (nodes:seq<Node>) = storage.Add nodes
    member x.Remove (nodes:seq<AddressBlock>) = storage.Remove nodes
    member x.Items (addressBlock:seq<AddressBlock>) = storage.Items addressBlock
    member x.First (predicate: (Node -> bool)) : System.Threading.Tasks.Task<Option<Node>> = storage.First predicate
    member x.Stop () = ()

module Utils =
    open Google.Protobuf

    let GetNodeIdHash (nodeid:NodeID) : NodeIdHash = { hash= nodeid.GetHashCode(); graph=nodeid.Graph; nodeid=nodeid.Nodeid }

    let metaPlainTextUtf8 = "xs:string"
    let metaXmlInt = "xs:int"
    let metaXmlDouble = "xs:double"
    let MetaBytes typ bytes = 
        let bb = new BinaryBlock()
        bb.Metabytes <- new TypeBytes()
        bb.Metabytes.Type <- typ
        bb.Metabytes.Bytes <- Google.Protobuf.ByteString.CopyFrom(bytes)
        bb
    
    let NullMemoryPointer() = 
        let p = new Grpc.MemoryPointer()
        p.Filename <- uint32 0
        p.Partitionkey <- uint32 0
        p.Offset <- uint64 0
        p.Length <- uint64 0
        p
    
    let Id graph nodeId (pointer:MemoryPointer) = 
        let ab = new AddressBlock()
        ab.Nodeid <- new NodeID()
        ab.Nodeid.Graph <- graph
        ab.Nodeid.Nodeid <- nodeId
        if (pointer = null) then
            ab.Nodeid.Pointer <- NullMemoryPointer () 
            ()
        else
            ab.Nodeid.Pointer <- pointer
            ()    
        ab       
        
    let BBString (text:string) =  MetaBytes metaPlainTextUtf8 (Text.UTF8Encoding.UTF8.GetBytes(text))
    let BBInt (value:int) =       MetaBytes metaXmlInt (BitConverter.GetBytes value)
    let BBDouble (value:double) = MetaBytes metaXmlDouble (BitConverter.GetBytes value)
    let DBA address =
        let data = new DataBlock()
        data.Address <- address
        data
    let DBB binary =
        let data = new DataBlock()
        data.Binary <- binary
        data        
    let DBBString (text:string) = 
        DBB (BBString text)
    let DBBInt (value:int) = 
        DBB (BBInt value)
    let DBBDouble (value:double) = 
        DBB (BBDouble value)
    let TMDAuto data = 
        let tmd = new TMD()
        tmd.Data <- data
        tmd
    let TMDTime data time =
        let tmd = TMDAuto data
        tmd.Timestamp <- time
        tmd 
             
    let Prop (key:DataBlock) (values:seq<DataBlock>) =
        let kv = new KeyValue()
        kv.Key <- TMDAuto key
        values
        |> Seq.map (fun x ->  TMDAuto x )   
        |> kv.Value.AddRange                        
        kv  
        
    let PropString (key:string) (values:seq<string>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBString x))  
    let PropInt (key:string) (values:seq<int>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBInt x))
    let PropDouble (key:string) (values:seq<double>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBDouble x))
    let PropData (key:string) (values:seq<DataBlock>) = Prop (DBBString key) values
    let Node key values = 
        let node = new Node()
        // TODO: let the number of reserved fragments be configurable
        let fragments = [|(NullMemoryPointer());(NullMemoryPointer()); (NullMemoryPointer())|]
        node.Id <- key
        node.Fragments.AddRange fragments
        node.Attributes.AddRange values
        node