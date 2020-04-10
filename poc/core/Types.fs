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

type Either<'L, 'R> =
    | Left of 'L
    | Right of 'R


type NodeIdHash = int //{ hash:int }

type IStorage =
    abstract member Nodes: seq<Node>
    abstract member Flush: unit -> unit
    abstract member Add: seq<Node> -> System.Threading.Tasks.Task
    abstract member Remove: seq<AddressBlock> -> System.Threading.Tasks.Task
    abstract member Items: seq<AddressBlock> -> System.Threading.Tasks.Task<seq<AddressBlock * Either<Node, Exception>>>
    abstract member First: (Node -> bool) -> System.Threading.Tasks.Task<Option<Node>> 
    abstract member Stop: unit -> unit

type NodeIdIndex (indexFile:string) = 
    //let ``Index of NodeID -> MemoryPointer`` = new System.Collections.Concurrent.ConcurrentDictionary<NodeIdHash, System.Collections.Generic.List<Grpc.MemoryPointer>>()
    let buffer = System.Buffers.ArrayPool<byte>.Shared 
    let path = Environment.ExpandEnvironmentVariables(indexFile)
    let options = (new DbOptions()).SetCreateIfMissing(true).EnableStatistics()
    let db = RocksDb.Open(options,path)
    let codec = FieldCodec.ForMessage<MemoryPointer>(18u, Ahghee.Grpc.MemoryPointer.Parser)
    let cleanup() = db.Dispose()

    let TryGetValueInternal (keybytes:array<byte>) (value: Pointers byref) = 
        let valueBytes = db.Get(keybytes)
        if valueBytes = null || valueBytes.Length = 0 then 
            value <- null
            false 
        else 
            let repeatedField = new Pointers()
            let codedinputStream = new CodedInputStream(valueBytes,0,valueBytes.Length)
            repeatedField.MergeFrom(codedinputStream)
            value <- repeatedField 
            true 

    override x.Finalize() =
        cleanup()

    interface IDisposable with
        member __.Dispose():unit = 
            cleanup()
            GC.SuppressFinalize(__);

    member __.TryGetValue (key:NodeIdHash, value: Pointers byref) = 
        let keybytes = BitConverter.GetBytes key
        TryGetValueInternal keybytes &value 
    
    member __.AddOrUpdateBatch (keys:byte[][]) (getValueForKey: byte[] -> Pointers) (update: (byte[] -> Pointers -> Pointers)) =
        let writeRP (rp:Pointers) = 
            let len = rp.CalculateSize()
            let b = Array.zeroCreate<byte>(len)
            let outputStream = new CodedOutputStream(b)
            rp.WriteTo(outputStream) 
            outputStream.Flush() 
            b
        
        //let wb = new ReadBatch()
        let keysBytes = keys
        
        let gotLots = db.MultiGet keysBytes
        // for keys with no values we just perform the Put operation.
        let (noValues, values) = 
            gotLots
            |> Array.partition ( fun kvp -> kvp.Value = null || kvp.Value.Length = 0)
        
        let writeBatch = new WriteBatch()
        // can use merge in writeBatch
        // for keys with values we do a Modify and Write. Eventually maybe do a rocksDb.merge instead of all of this
                
        noValues
            |> Array.iter (fun kvp -> writeBatch.Put(kvp.Key, getValueForKey(kvp.Key) |> writeRP ) |> ignore)
        
        values
            |> Array.iter (fun kvp -> 
                    let mutable outValue:Pointers = null
                    let success = TryGetValueInternal kvp.Key (& outValue)
                    let newVal = 
                        if success then
                            let updated = update kvp.Key outValue
                            updated |> writeRP
                        else
                            getValueForKey(kvp.Key) |> writeRP
                    writeBatch.Put(kvp.Key, newVal) |> ignore
                )    
        db.Write(writeBatch)
        
        ()
             
    member __.AddOrUpdate (key:NodeIdHash) (value: (Unit -> Pointers)) (update: (NodeIdHash -> Pointers -> Pointers)) =
        let keybytes = BitConverter.GetBytes key
        let writeRP (rp:Pointers) = 
            let len = rp.CalculateSize()
            let b = buffer.Rent(len)
            let outputStream = new CodedOutputStream(b)
            rp.WriteTo(outputStream) 
            outputStream.Flush() 
            db.Put(keybytes,keybytes.LongLength,b,int64 len)
            buffer.Return b
            rp
       
        let mutable outValue:Pointers = null
        let success = TryGetValueInternal keybytes (& outValue)
        if success then
            let updated = update key outValue
            writeRP updated
        else
            writeRP (value())
    member __.AddOrUpdateCS (key:NodeIdHash) (value: Func<Pointers>) (update: Func<NodeIdHash,Pointers,Pointers>) =
        __.AddOrUpdate key (fun () -> value.Invoke()) (fun a b -> update.Invoke(a,b))

module Utils =
    open Google.Protobuf

    let GetNodeIdHash (nodeid:NodeID) : NodeIdHash =  nodeid.GetHashCode() 
    let GetAddressBlockHash (ab:AddressBlock) : NodeIdHash =
        let nid = 
            match ab.AddressCase with 
            | AddressBlock.AddressOneofCase.Globalnodeid -> ab.Globalnodeid.Nodeid
            | AddressBlock.AddressOneofCase.Nodeid -> ab.Nodeid
            | _ -> raise (new NotImplementedException("AddressBlock did not contain a valid NodeID"))
        GetNodeIdHash nid    
    let GetPartitionFromHash (partitionCount:int) (nodeHash:NodeIdHash) =
        int ((uint32 nodeHash) % uint32 partitionCount)

    let metaPlainTextUtf8 = "xs:string"
    let metaXmlInt = "xs:int"
    let metaXmlDouble = "xs:double"
    let MetaBytesNoCopy typ bytes = 
        let bb = new BinaryBlock()
        bb.Metabytes <- new TypeBytes()
        bb.Metabytes.Type <- typ
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
        
    let BBString (text:string) =  MetaBytesNoCopy metaPlainTextUtf8 (ByteString.CopyFromUtf8 text)
    let BBInt (value:int) =       MetaBytes metaXmlInt ( BitConverter.GetBytes value)
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