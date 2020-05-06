namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Generic
open System.Data.SqlTypes
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc
open RocksDbSharp
open System
open System.Buffers
open System.Linq
open System.Threading
open Microsoft.FSharp.NativeInterop
open System.Runtime.InteropServices
open System.Runtime.InteropServices

module Utils =
    open Google.Protobuf

    let GetPartitionFromHash (partitionCount:int) (nid:NodeID) =
        int ((uint32 <| nid.GetHashCode()) % uint32 partitionCount)

    let metaPlainTextUtf8 = "xs:string"
    let metaXmlInt = "xs:int"
    let metaXmlDouble = "xs:double"
    let MetaBytesNoCopy typ bytes = 
        let bb = new DataBlock()
        bb.Metabytes <- new TypeBytes()
        bb.Metabytes.Typeiri <- typ
        bb.Metabytes.Bytes <- bytes
        bb
    
    let MetaBytes typ (bytes: byte[]) = 
        MetaBytesNoCopy typ (Google.Protobuf.ByteString.CopyFrom(bytes))
    
    let NullMemoryPointer() = 
        let p = new Grpc.MemoryPointer()
        p.Filename <- uint32 0
        p.Partitionkey <- uint32 0
        p.Offset <- uint64 0
        p.Length <- uint64 0
        p
    
    let Id graph nodeId (pointer:MemoryPointer) = 
        let Nodeid = new NodeID()
        Nodeid.Remote <- graph
        Nodeid.Iri <- nodeId
        if (pointer = null) then
            Nodeid.Pointer <- NullMemoryPointer () 
            ()
        else
            Nodeid.Pointer <- pointer
            ()    
        Nodeid       
        
    let DBA address =
        let data = new DataBlock()
        data.Nodeid <- address
        data
    let DBBEmpty () =
        let data = new DataBlock()
        data
    let DBBString (text:string) = 
        let data = new DataBlock()
        data.Str <- text
        data
    let DBBInt (value:int) =
        let data = new DataBlock()
        data.I32 <- value
        data
    let DBBDouble (value:double) = 
        let data = new DataBlock()
        data.D <- value
        data
        
    let DBBFloat (value: float32) =
        let data = new DataBlock()
        data.F <- value
        data
     
    let TMDAuto data = 
        let tmd = new TMD()
        tmd.Data <- data
        tmd
    let TMDTime data time =
        let tmd = TMDAuto data
        tmd.Timestamp <- time
        tmd 
             
    let Prop (key:DataBlock) (value:DataBlock) =
        let kv = new KeyValue()
        kv.Key <- TMDAuto key
        kv.Value <- TMDAuto value
        kv  
        
    let PropString (key:string) (value:string) = Prop (DBBString key) (value |> DBBString )  
    let PropInt (key:string) (value:int) = Prop (DBBString key) (value |> DBBInt )
    let PropDouble (key:string) (value:double) = Prop (DBBString key) (value |> DBBDouble )
    let PropData (key:string) (value:DataBlock) = Prop (DBBString key) value
        
    let Node key (values:seq<KeyValue>) = 
        let node = new Node()
        // TODO: let the number of reserved fragments be configurable
        node.Id <- key
        node.Fragments.Add (NullMemoryPointer())
        node.Fragments.Add (NullMemoryPointer())
        node.Fragments.Add (NullMemoryPointer())
        
        for v in values do
            node.Attributes.Add v
        node        