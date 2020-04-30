
namespace Ahghee

    
module public TinkerPop =
    open Utils
    open FSharp.Data
    open System.Text
    open System
    open Ahghee.Grpc
    
    type public GraphML = XmlProvider<"""https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml""">
    let TheCrew = lazy ( GraphML.Load("tinkerpop-modern.xml") )
  
    let ABtoyId id : NodeID =
            let ab graph i= 
                let Nodeid = NodeID()
                Nodeid.Pointer <- NullMemoryPointer()
                Nodeid.Remote <- graph
                Nodeid.Iri <- i
                Nodeid
            ab "TheCrew" id
    
    let DABtoyId id : DataBlock =
        DBA (ABtoyId id)
            
    let xsType graphMlType : string =
        match graphMlType with
        | "string" -> metaPlainTextUtf8
        | "int" -> metaXmlInt
        | "double" -> metaXmlDouble
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
        
        let Id id = 
            Id "" id (NullMemoryPointer())
        
        let buildNodesFromGraphMlNodes (nodes:seq<GraphML.Node>) (edges:seq<GraphML.Edge>) = 
            nodes
            |> Seq.map (fun n -> 
                     
                        let z = new Grpc.Node()
                        z.Id <- Id (n.Id.ToString());  
                        n.Datas
                            |> Seq.ofArray
                            |> Seq.map (fun d ->
                                let (keyString, valueMeta) = 
                                    let (name, typ) = NodeAttrs.Item d.Key
                                    name, xsType typ
                                
                                let valueBytes =
                                    match valueMeta with
                                    | m when m = metaPlainTextUtf8 -> match d.String with  
                                                                       | Some(s) -> DBBString s
                                                                       | _ -> DBBEmpty()
                                    | m when m = metaXmlDouble -> match d.String with  
                                                                   | Some(s) -> DBBDouble (double s)
                                                                   | _ -> DBBEmpty()
                                    | m when m = metaXmlInt -> match d.String with  
                                                                | Some(s) -> DBBInt (int32 s)
                                                                | _ -> DBBEmpty()
                                    | _ -> DBBEmpty()                                                                                   
                                
                                let kv1 = new KeyValue()
                                kv1.Key <- TMDAuto (DBBString keyString)
                                kv1.Value <- valueBytes |> TMDAuto
                                kv1
                            )
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Source = n.Id)
                                             |> Seq.map (fun e ->
                                                            Prop (DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "out." + d.String.Value)
                                                                                         ))
                                                                 (DABtoyId (e.Id.ToString()))                     
                                                         )
                                             ) 
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Target = n.Id)
                                             |> Seq.map (fun e -> 
                                                            Prop (DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "in." + d.String.Value)
                                                                                         ))
                                                                 (DABtoyId (e.Id.ToString()))                     
                                                         )
                                             )                  
                        |> z.Attributes.AddRange
                        z)
                
        let buildEdgeNodesFromGraphMlEdges (edges:seq<GraphML.Edge>) = 
            edges
            |> Seq.map (fun n -> 
                        let z = new Grpc.Node()
                        z.Id <-  Id (n.Id.ToString()) ;  
                        n.Datas
                            |> Seq.ofArray
                            |> Seq.map (fun d ->
                                let (keyString, valueMeta) = 
                                    let (name, typ) = EdgeAttrs.Item d.Key
                                    name, xsType typ
                                
                                let valueBytes =
                                    match valueMeta with
                                    | m when m = metaPlainTextUtf8 -> match d.String with  
                                                                       | Some(s) -> DBBString s
                                                                       | _ -> DBBEmpty()
                                    | m when m = metaXmlDouble -> match d.String with  
                                                                   | Some(s) -> DBBDouble (double s)
                                                                   | _ -> DBBEmpty()
                                    | m when m = metaXmlInt -> match d.String with  
                                                                | Some(s) -> DBBInt (int32 s)
                                                                | _ -> DBBEmpty()
                                    | _ -> DBBEmpty()                                                                                   
                                
                                let kv1 = new KeyValue()
                                kv1.Key <- TMDAuto (DBBString keyString)
                                kv1.Value <- valueBytes |> TMDAuto
                                kv1
                            )
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Source = n.Id)
                                             |> Seq.map (fun e -> 
                                                            Prop (DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "out." + d.String.Value)
                                                                                         ))
                                                                (DABtoyId (e.Id.ToString()))                  
                                                         )
                                             ) 
                            |> Seq.append (edges
                                             |> Seq.filter (fun e -> e.Target = n.Id)
                                             |> Seq.map (fun e -> 
                                                            Prop (DBBString (e.Datas 
                                                                                         |> Seq.find (fun d -> d.Key = "labelE")
                                                                                         |> (fun d -> "in." + d.String.Value)
                                                                                         ))
                                                                (DABtoyId (e.Id.ToString()))
                                                         )
                                             )
                            |> Seq.append ( [ 
                                                Prop (DBBString "source") (DABtoyId (n.Source.ToString()))
                                                Prop (DBBString "target") (DABtoyId (n.Target.ToString()))
                                            ] ) 
                        |> z.Attributes.AddRange
                        z)
        
        buildNodesFromGraphMlNodes gmlGraph.Nodes gmlGraph.Edges
        |> Seq.append (buildEdgeNodesFromGraphMlEdges gmlGraph.Edges)
    
    let buildNodesFromFile(filename:String) =
        buildNodesFromGraphML (GraphML.Load(filename))
    let buildNodesTheCrew : seq<Node> =
        buildNodesFromGraphML (TheCrew.Value)