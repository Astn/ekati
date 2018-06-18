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
    let path = Environment.ExpandEnvironmentVariables(indexFile)
    let options = (new DbOptions()).SetCreateIfMissing(true).EnableStatistics()
    let db = RocksDb.Open(options,path)
    let codec = FieldCodec.ForMessage<MemoryPointer>(18u, Ahghee.Grpc.MemoryPointer.Parser)
    let cleanup() = db.Dispose()

    let TryGetValueInternal (keybytes:array<byte>) (value: RepeatedField<Grpc.MemoryPointer> byref) = 
        let valueBytes = db.Get(keybytes)
        if valueBytes = null || valueBytes.Length = 0 then 
            value <- null
            false 
        else 
            let repeatedField = new RepeatedField<Grpc.MemoryPointer>()
            let codedinputStream = new CodedInputStream(valueBytes,0,valueBytes.Length)
            let len = codedinputStream.ReadLength() // ?? maybe...
            repeatedField.AddEntriesFrom(codedinputStream, codec)
            value <- repeatedField 
            true 

    override x.Finalize() =
        cleanup()

    interface IDisposable with
        member __.Dispose():unit = 
            cleanup()
            GC.SuppressFinalize(__);

    member __.TryGetValue (key:NodeIdHash, value: RepeatedField<Grpc.MemoryPointer> byref) = 
        let keybytes = BitConverter.GetBytes key
        TryGetValueInternal keybytes &value 
             
    member __.AddOrUpdate (key:NodeIdHash) (value: System.Collections.Generic.IList<Grpc.MemoryPointer>) (update: (NodeIdHash -> System.Collections.Generic.IList<Grpc.MemoryPointer> -> System.Collections.Generic.IList<Grpc.MemoryPointer>)) =
        let keybytes = BitConverter.GetBytes key
        let writeRP (rp:RepeatedField<Grpc.MemoryPointer>) = 
            let len = (rp.CalculateSize(codec))
            let buffer = Array.zeroCreate<byte> (len + 4) 
            let outputStream = new CodedOutputStream(buffer)
            //outputStream.WriteLength len
            rp.WriteTo(outputStream, codec)
            outputStream.WriteRawTag (byte 10) 
            outputStream.Flush() 
            db.Put(keybytes,buffer)
            rp
        
        let writeIList (lst:IList<Grpc.MemoryPointer>) =
            match lst with 
            | :? RepeatedField<Grpc.MemoryPointer> as rp -> writeRP rp
            | :? IList<Grpc.MemoryPointer> as list -> 
                let rp = new RepeatedField<Grpc.MemoryPointer>()
                for l in list do
                    rp.Add l
                writeRP rp
        
        let mutable outValue:RepeatedField<Grpc.MemoryPointer> = null
        let success = TryGetValueInternal keybytes (& outValue)
        if success then
            let updated = update key outValue
            writeIList updated
        else
            writeIList value
    member __.AddOrUpdateCS (key:NodeIdHash) (value: System.Collections.Generic.IList<Grpc.MemoryPointer>) (update: Func<NodeIdHash,System.Collections.Generic.IList<Grpc.MemoryPointer>,System.Collections.Generic.IList<Grpc.MemoryPointer>>) =
        __.AddOrUpdate key value (fun a b -> update.Invoke(a,b))
//    public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
//    {
//      if ((object) key == null)
//        ConcurrentDictionary<TKey, TValue>.ThrowKeyNullException();
//      if (updateValueFactory == null)
//        throw new ArgumentNullException(nameof (updateValueFactory));
//      int hashCode = this._comparer.GetHashCode(key);
//      TValue comparisonValue;
//      TValue newValue;
//      do
//      {
//        while (!this.TryGetValueInternal(key, hashCode, out comparisonValue))
//        {
//          TValue resultingValue;
//          if (this.TryAddInternal(key, hashCode, addValue, false, true, out resultingValue))
//            return resultingValue;
//        }
//        newValue = updateValueFactory(key, comparisonValue);
//      }
//      while (!this.TryUpdateInternal(key, hashCode, newValue, comparisonValue));
//      return newValue;
//    }    

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
    
    let MetaBytes typ bytes = 
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