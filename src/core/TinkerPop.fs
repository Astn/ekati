
namespace Ekati

open Ekati.Core
open FlatBuffers
open Ekati.Ext
    
module public TinkerPop =
    open FSharp.Data
    open System.Text
    open System
    
    type public GraphML = XmlProvider<"""https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml""">
    let TheCrew = lazy ( GraphML.Load("tinkerpop-modern.xml") )
  
    let ABtoyId buf (id:string) : Offset<NodeID> =
        NodeID.CreateNodeID(buf, "".CopyTo(buf), id.CopyTo(buf))
            
    let xsType graphMlType : string =
        match graphMlType with
        | "string" -> Utils.metaPlainTextUtf8
        | "int" -> Utils.metaXmlInt
        | "double" -> Utils.metaXmlDouble
        | _ -> ""
    
    
        
    let buildNodesFromGraphML (gmlfile : GraphML.Graphml) : seq<Node> =
        let gmlGraph = gmlfile.Graph
        let attrs forType= 
            gmlfile.Keys
            |> Seq.ofArray
            |> Seq.filter (fun k -> k.For = forType)
            |> Seq.map (fun k -> k.Id, (k.AttrName, k.AttrType))
            |> Map.ofSeq
        let NodeAttrs = attrs "node" 
        let EdgeAttrs = attrs "edge" 
       
        let buildNodesFromGraphMlNodes (nodes:seq<GraphML.Node>) (edges:seq<GraphML.Edge>) = 
            let now = DateTime.UtcNow.Ticks
            nodes
            |> Seq.map (fun n -> 
                        let buf = FlatBufferBuilder(128)
                        
                        let nid = ABtoyId buf (n.Id.ToString())  
                        let attrs = n.Datas
                                    |> Seq.ofArray
                                    |> Seq.map (fun d ->
                                        let (keyString, valueMeta) = 
                                            let (name, typ) = NodeAttrs.Item d.Key
                                            name, xsType typ
                                        
                                        let valueBytes =
                                            match valueMeta with
                                            | m when m = Utils.metaPlainTextUtf8 -> match d.String with  
                                                                               | Some(s) -> Utils.PropString(buf ,keyString ,s, now)
                                                                               | _ -> Utils.PropString (buf  ,keyString , "", now)
                                            | m when m = Utils.metaXmlDouble -> match d.String with  
                                                                           | Some(s) -> Utils.PropDouble(buf, keyString, (double s), now)
                                                                           | _ -> Utils.PropDouble(buf ,keyString ,(double 0.0),now)
                                            | m when m = Utils.metaXmlInt -> match d.String with  
                                                                        | Some(s) -> Utils.PropInt(buf ,keyString ,(int32 s),now)
                                                                        | _ -> Utils.PropInt (buf ,keyString ,(int32 0),now)
                                            | _ ->  Utils.PropInt(buf, keyString, (int32 0)                                                                               , now)
                                        
                                        valueBytes 
                                    )
                                    |> Seq.append (edges
                                                     |> Seq.filter (fun e -> e.Source = n.Id)
                                                     |> Seq.map (fun e ->
                                                                    Utils.PropData( buf, (e.Datas 
                                                                                                 |> Seq.find (fun d -> d.Key = "labelE")
                                                                                                 |> (fun d -> "out." + d.String.Value)
                                                                                                 ),
                                                                         (Data.CreateData(buf, DataBlock.NodeID, (ABtoyId buf (e.Id.ToString())).Value)), now                 )
                                                                 )
                                                     ) 
                                    |> Seq.append (edges
                                                     |> Seq.filter (fun e -> e.Target = n.Id)
                                                     |> Seq.map (fun e -> 
                                                                    Utils.PropData(buf ,(e.Datas 
                                                                                                 |> Seq.find (fun d -> d.Key = "labelE")
                                                                                                 |> (fun d -> "in." + d.String.Value)
                                                                                                 ),
                                                                         (Data.CreateData(buf, DataBlock.NodeID, (ABtoyId buf (e.Id.ToString())).Value)), now                  )
                                                                 )
                                                     )
                                    |> Seq.toArray
                        let node = Ekati.Core.Node.CreateNode(buf, nid, Map.CreateMap(buf, Map.CreateItemsVector(buf, attrs)))
                        buf.Finish(node.Value)
                        Ekati.Core.Node.GetRootAsNode(buf.DataBuffer)
                        )
                
        let buildEdgeNodesFromGraphMlEdges (edges:seq<GraphML.Edge>) = 
            let now = DateTime.UtcNow.Ticks
            edges
            |> Seq.map (fun n ->
                        let buf = FlatBufferBuilder(128)
                        
                        let nid = ABtoyId buf (n.Id.ToString())  
                        
                        let attrs = n.Datas
                                    |> Seq.ofArray
                                    |> Seq.map (fun d ->
                                        let (keyString, valueMeta) = 
                                            let (name, typ) = EdgeAttrs.Item d.Key
                                            name, xsType typ
                                        
                                        let valueBytes =
                                            match valueMeta with
                                            | m when m = Utils.metaPlainTextUtf8 -> match d.String with  
                                                                               | Some(s) -> Utils.PropString( buf ,keyString, s,now)
                                                                               | _ -> Utils.PropString (buf  ,keyString  ,"", now)
                                            | m when m = Utils.metaXmlDouble -> match d.String with  
                                                                           | Some(s) -> Utils.PropDouble(buf ,keyString ,(double s), now)
                                                                           | _ -> Utils.PropString (buf  ,keyString  ,"", now)
                                            | m when m = Utils.metaXmlInt -> match d.String with  
                                                                        | Some(s) -> Utils.PropInt (buf ,keyString ,(int32 s), now)
                                                                        | _ -> Utils.PropString (buf  ,keyString  ,"", now)
                                            | _ -> Utils.PropInt (buf ,keyString ,(int32 0)                                                                                , now)
                                        
                                        valueBytes
                                    )
                                    |> Seq.append (edges
                                                     |> Seq.filter (fun e -> e.Source = n.Id)
                                                     |> Seq.map (fun e -> 
                                                                    Utils.PropData (buf, (e.Datas 
                                                                                                 |> Seq.find (fun d -> d.Key = "labelE")
                                                                                                 |> (fun d -> "out." + d.String.Value)
                                                                                                 ),
                                                                        (Data.CreateData(buf, DataBlock.NodeID, (ABtoyId buf (e.Id.ToString())).Value)), now                  )
                                                                 )
                                                     ) 
                                    |> Seq.append (edges
                                                     |> Seq.filter (fun e -> e.Target = n.Id)
                                                     |> Seq.map (fun e -> 
                                                                    Utils.PropData (buf ,(e.Datas 
                                                                                                 |> Seq.find (fun d -> d.Key = "labelE")
                                                                                                 |> (fun d -> "in." + d.String.Value)
                                                                                                 ),
                                                                        (Data.CreateData(buf, DataBlock.NodeID, (ABtoyId buf (e.Id.ToString())).Value)), now)
                                                                 )
                                                     )
                                    |> Seq.append ( [ 
                                                        Utils.PropData( buf, "source", (Data.CreateData(buf, DataBlock.NodeID, (ABtoyId buf (n.Source.ToString())).Value)), now)
                                                        Utils.PropData( buf, "target", (Data.CreateData(buf, DataBlock.NodeID, (ABtoyId buf (n.Target.ToString())).Value)), now)
                                                    ] )
                                    |> Seq.toArray
                        let node = Ekati.Core.Node.CreateNode(buf, nid, Map.CreateMap(buf, Map.CreateItemsVector(buf, attrs)))
                        buf.Finish(node.Value)
                        Ekati.Core.Node.GetRootAsNode(buf.DataBuffer)
                        )
        
        buildNodesFromGraphMlNodes gmlGraph.Nodes gmlGraph.Edges
        |> Seq.append (buildEdgeNodesFromGraphMlEdges gmlGraph.Edges)
    
    let buildNodesFromFile(filename:String) =
        buildNodesFromGraphML (GraphML.Load(filename))
    let buildNodesTheCrew : seq<Node> =
        buildNodesFromGraphML (TheCrew.Value)