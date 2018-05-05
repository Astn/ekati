namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open Microsoft.AspNetCore.Mvc
open System
open System.Threading
open System.Threading.Tasks

// Node Block
// |----------
// | Ids : Seq<AddressBlock>
// | Attrs: Seq<KeyValue>
// |----------

// KeyValue
// |---------
// | Key: Data 
// | Value: Seq<Data>
// |---------

// Data: (AddressBlock | BinaryBlock)

// Address Block
// |-----------
// | (NodeID | MemoryPointer )
// |-----------

// Binary Block
// |----------
// | (MetaBytes | MemoryPointer )
// |----------

type MemoryPointer = { 
    PartitionKey: string
    FileName: string; 
    offset: int64; 
    length: int64 
    }
           
type MetaBytes = { 
    Meta: Option<string>; 
    Bytes : Byte[] 
    }

type NodeID = { 
    Graph: string 
    NodeId: string 
    Pointer: Option<MemoryPointer>
    } 
    
type GlobalNodeID = { 
    Domain: string
    Database: string 
    NodeId: NodeID
    }     
     
type BinaryBlock =
    | MetaBytes of MetaBytes
    | MemoryPointer of MemoryPointer

type AddressBlock =
    | NodeID of NodeID
    | GlobalNodeID of GlobalNodeID

// TODO: Add timestamp for versioning
type Data =
  | AddressBlock of AddressBlock
  | BinaryBlock of BinaryBlock

  
type KeyValue = { Key: Data; Value : seq<Data> }
type Node = { NodeIDs: seq<AddressBlock>; Attributes: seq<KeyValue> }

type Either<'L, 'R> =
    | Left of 'L
    | Right of 'R

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
    let metaPlainTextUtf8 = Some("xs:string")
    let metaXmlInt = Some("xs:int")
    let metaXmlDouble = Some("xs:double")
    
    let ABTestId id = AddressBlock.NodeID { Graph="People"; NodeId=id; Pointer=None;}
    let BBString (text:string) = BinaryBlock.MetaBytes { Meta = metaPlainTextUtf8 ; Bytes = Text.UTF8Encoding.UTF8.GetBytes(text) }
    let BBInt (value:int) = BinaryBlock.MetaBytes { Meta = metaXmlInt ; Bytes = BitConverter.GetBytes value }
    let BBDouble (value:double) = BinaryBlock.MetaBytes { Meta = metaXmlDouble ; Bytes = BitConverter.GetBytes value }
    
    let DABTestId id = Data.AddressBlock (ABTestId id)    
    let DBBString (text:string) = Data.BinaryBlock (BBString text)
    let DBBInt (value:int) = Data.BinaryBlock (BBInt value)
    let DBBDouble (value:double) = Data.BinaryBlock (BBDouble value)
    let Prop (key:Data) (values:seq<Data>) =
        let pair =  { KeyValue.Key = key; Value = (values |> Seq.toArray)}   
        pair  
        
    let PropString (key:string) (values:seq<string>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBString x))  
    let PropInt (key:string) (values:seq<int>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBInt x))
    let PropDouble (key:string) (values:seq<double>) = Prop (DBBString key) (values |> Seq.map(fun x -> DBBDouble x))
    let PropData (key:string) (values:seq<Data>) = Prop (DBBString key) values
    
module TinkerPop =
    open Utils
    open FSharp.Data
    open System.Text
    
    type GraphML = XmlProvider<"""https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml""">
    let TheCrew = lazy ( GraphML.Load("https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml") )

    
    let xsType graphMlType : Option<string> =
        match graphMlType with
        | "string" -> metaPlainTextUtf8
        | "int" -> metaXmlInt
        | "double" -> metaXmlDouble
        | _ -> None
        
    let buildNodesTheCrew : seq<Node> =
        let attrs forType= 
            TheCrew.Value.Keys 
            |> Seq.ofArray
            |> Seq.filter (fun k -> k.For = forType)
            |> Seq.map (fun k -> k.Id, (k.AttrName, k.AttrType))
            |> Map.ofSeq
        let NodeAttrs = attrs "node" 
        let EdgeAttrs = attrs "edge" 
        
        let Id id = AddressBlock.NodeID { Graph="TheCrew"; NodeId=id; Pointer=None;}
        
        let buildNodesFromGraphMlNodes (nodes:seq<GraphML.Node>) (edges:seq<GraphML.Edge>) = 
            nodes
            |> Seq.map (fun n -> 
                     { 
                        Node.NodeIDs = [| Id (n.Id.ToString()) |];  
                        Node.Attributes = 
                            n.Datas
                            |> Seq.ofArray
                            |> Seq.map (fun d ->
                                let keyBytes = 
                                    let (name, typ) = NodeAttrs.Item d.Key
                                    Encoding.UTF8.GetBytes name
                                
                                let valueMeta =
                                    let (name, typ) = NodeAttrs.Item d.Key 
                                    xsType typ
                                
                                let valueBytes =
                                    match valueMeta with
                                    | m when m = metaPlainTextUtf8 -> match d.String with  
                                                                       | Some(s) -> Encoding.UTF8.GetBytes s
                                                                       | _ -> Array.empty<byte>
                                    | m when m = metaXmlDouble -> match d.String with  
                                                                   | Some(s) -> BitConverter.GetBytes (double s)
                                                                   | _ -> Array.empty<byte>
                                    | m when m = metaXmlInt -> match d.String with  
                                                                | Some(s) -> BitConverter.GetBytes (int32 s)
                                                                | _ -> Array.empty<byte>
                                    | _ -> Array.empty<byte>                                                                                    
                                
                                { 
                                    KeyValue.Key= BinaryBlock (MetaBytes {
                                                                        MetaBytes.Meta= metaPlainTextUtf8;
                                                                        Bytes= keyBytes
                                                                             })
                                    Value= [BinaryBlock (MetaBytes {
                                                                    MetaBytes.Meta= valueMeta
                                                                    Bytes= valueBytes})]
                                }
                            )
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Source = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "out." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             ) 
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Target = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "in." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             )                  
                     }
                )
                
        let buildEdgeNodesFromGraphMlEdges (edges:seq<GraphML.Edge>) = 
            edges
            |> Seq.map (fun n -> 
                     { 
                        Node.NodeIDs = [| Id (n.Id.ToString()) |];  
                        Node.Attributes = 
                            n.Datas
                            |> Seq.ofArray
                            |> Seq.map (fun d ->
                                let keyBytes = 
                                    let (name, typ) = EdgeAttrs.Item d.Key
                                    Encoding.UTF8.GetBytes name
                                
                                let valueMeta =
                                    let (name, typ) = EdgeAttrs.Item d.Key 
                                    xsType typ
                                
                                let valueBytes =
                                    match valueMeta with
                                    | m when m = metaPlainTextUtf8 -> match d.String with  
                                                                       | Some(s) -> Encoding.UTF8.GetBytes s
                                                                       | _ -> Array.empty<byte>
                                    | m when m = metaXmlDouble -> match d.String with  
                                                                   | Some(s) -> BitConverter.GetBytes (double s)
                                                                   | _ -> Array.empty<byte>
                                    | m when m = metaXmlInt -> match d.String with  
                                                                | Some(s) -> BitConverter.GetBytes (int32 s)
                                                                | _ -> Array.empty<byte>
                                    | _ -> Array.empty<byte>                                                                                    
                                
                                { 
                                    KeyValue.Key= BinaryBlock (MetaBytes {
                                                                        MetaBytes.Meta= metaPlainTextUtf8;
                                                                        Bytes= keyBytes
                                                                             })
                                    Value= [BinaryBlock (MetaBytes {
                                                                    MetaBytes.Meta= valueMeta
                                                                    Bytes= valueBytes})]
                                }
                            )
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Source = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "out." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             ) 
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Target = n.Id)
                                             |> Seq.map (fun e -> 
                                                            {
                                                                KeyValue.Key = DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "in." + d.String.Value)
                                                                                         );
                                                                Value = [DABTestId (e.Id.ToString())]
                                                            }                         
                                                         )
                                             )
                            |> Seq.append ( [ 
                                                { KeyValue.Key= DBBString "source"; Value= [DABTestId (n.Source.ToString())]}
                                                { KeyValue.Key= DBBString "target"; Value= [DABTestId (n.Target.ToString())]}
                                            ] )          
                     }
                )
        
        buildNodesFromGraphMlNodes TheCrew.Value.Graph.Nodes TheCrew.Value.Graph.Edges
        |> Seq.append (buildEdgeNodesFromGraphMlEdges TheCrew.Value.Graph.Edges)
        