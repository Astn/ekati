module Tests

open System
open Xunit
open Xunit.Abstractions
open Ahghee
open Ahghee.Grpc
open Ahghee.Utils
open Ahghee.TinkerPop
open System
open System.Collections
open System.IO
open System.Text
open System.Threading.Tasks
open FSharp.Data


type StorageType =
    | Memory
    | GrpcFile
    

type MyTests(output:ITestOutputHelper) =
    [<Fact>]
    member __.``Can create an InternalIRI type`` () =
        let id = DBA ( ABTestId "1" ) 
        let success = match id.DataCase with 
                        | DataBlock.DataOneofCase.Address-> true
                        | _ -> false 
        Assert.True(success)  
        
    [<Fact>]
    member __.``Can create a Binary type`` () =
        let d = DBB ( MetaBytes metaPlainTextUtf8 (Array.Empty<byte>()))
        let success = match d.DataCase with 
                        | DataBlock.DataOneofCase.Binary -> true
                        | _ -> false
        Assert.True(success)   
    
   
        
    [<Fact>]
    member __.``Can create a Pair`` () =
        let pair = PropString "firstName" [|"Richard"; "Dick"|]
        let md = pair.Key.Data
        let success = match md.DataCase with
                        | DataBlock.DataOneofCase.Binary when md.Binary.Metabytes.Type = metaPlainTextUtf8 -> true 
                        | _ -> false
        Assert.True success     
        
    [<Fact>]
    member __.``Can create a Node`` () =
        let node = Node [| ABTestId "1" |] 
                        [| PropString "firstName" [|"Richard"; "Dick"|] |]
    
        let empty = node.Attributes |> Seq.isEmpty
        Assert.True (not empty)
    
    
    
    member __.buildNodes : seq<Node> = 
        let node1 = Node [| ABTestId "1" |] 
                         [|
                            PropString "firstName" [|"Richard"; "Dick"|] 
                            PropData "follows" [| DABTestId "2" |] 
                         |]
                   
        let node2 = Node [| ABTestId "2" |] 
                         [|
                            PropString "firstName" [|"Sam"; "Sammy"|] 
                            PropData "follows" [| DABTestId "1" |]
                         |]
                   
        let node3 = Node [| ABTestId "3" |] 
                         [|
                            PropString "firstName" [|"Jim"|]
                            PropData "follows" [| DABTestId "1"; DABTestId "2" |] 
                         |]
                                      
        [| node1; node2; node3 |]      
        |> Array.toSeq             
    
    
    
    member __.buildGraph : Graph =
        let g:Graph = new Graph(new MemoryStore())
        let nodes = __.buildNodes
        let task = g.Add nodes
        match task.Status with
        | TaskStatus.Created -> task.Start()
        | _ -> ()                                                                     
        task.Wait()
        g
        
    member __.toyGraph : Graph =
        let g:Graph = new Graph(new MemoryStore())
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
        task.Wait()
        g

    [<Theory>]
    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>]
    member __.``Can Add nodes to graph`` (storeType) =
        let g:Graph = 
            match storeType with 
            | "StorageType.Memory" ->   new Graph(new MemoryStore())
            | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                    Config.ParitionCount=12; 
                                                                    log = (fun msg -> output.WriteLine msg)
                                                                    DataDirectoryPostfix="c" 
                                                                    }))
            | _ -> raise <| new NotImplementedException()                                                                    
            
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        output.WriteLine <| sprintf "task is: %A" task.Status
        let result = task.Wait(10000)
        g.Stop()
        output.WriteLine <| sprintf "task is now : %A" task.Status
        Assert.Equal( TaskStatus.RanToCompletion, task.Status)
        Assert.Equal( task.IsCompletedSuccessfully, true)
        ()

    [<Fact>]
    member __.``ChooseNodePartition is consistent`` () =
        let store = new GrpcFileStore({
                                        Config.ParitionCount=12;
                                        log = (fun msg -> output.WriteLine msg)
                                        DataDirectoryPostfix="a" 
                                        })
                                      
        let nodes = buildNodesTheCrew
        let expected = nodes    
                        |> Seq.map (fun n -> store.ChooseNodePartition n)
        let actual1 = nodes    
                        |> Seq.map (fun n -> store.ChooseNodePartition n)
        
        (store :> IStorage).Stop |> ignore               
        let store2 = new GrpcFileStore({
                                        Config.ParitionCount=12
                                        log = (fun msg -> output.WriteLine msg)
                                        DataDirectoryPostfix="b" 
                                        })                          
        
        let actual2 = nodes    
                        |> Seq.map (fun n -> store2.ChooseNodePartition n)
        (store2 :> IStorage).Stop |> ignore                        
        
        Assert.Equal<int>(expected,actual1)
        Assert.Equal<int>(expected,actual2)                                               
                        
                                                

    [<Fact>]
    member __.``Can Remove nodes from graph`` () =
        let g:Graph = __.buildGraph

        let n1 = g.Nodes
        let len1 = n1 |> Seq.length
        
        let toRemove = n1 
                        |> Seq.head 
                        |> (fun n -> n.Ids)
        let task = g.Remove(toRemove)
        task.Wait()
        let n2 = g.Nodes
        let len2 = n2 |> Seq.length                        
        
        Assert.NotEqual<Node> (n1, n2)
        output.WriteLine("len1: {0}; len2: {1}", len1, len2)
        (len1 = len2 + 1) |> Assert.True 
    
        
    [<Fact>]
    member __.``Can traverse local graph index`` () =
        let g = __.buildGraph
        let nodesWithIncomingEdges = g.Nodes 
                                         |> Seq.collect (fun n -> n.Attributes) 
                                         |> Seq.collect (fun y -> y.Value 
                                                                  |> Seq.map (fun x -> match x.Data.DataCase with  
                                                                                        | DataBlock.DataOneofCase.Address -> 
                                                                                            Some(x.Data.Address) 
                                                                                        | _ -> None))   
                                         |> Seq.filter (fun x -> match x with 
                                                                 | Some id -> true 
                                                                 | _ -> false)
                                         |> Seq.map    (fun x -> x.Value )
                                         |> Seq.distinct
                                         |> g.Items 

        Assert.NotEmpty nodesWithIncomingEdges.Result
    
    [<Fact>] 
    member __.``Can get IDs after load tinkerpop-crew.xml into graph`` () =
         let g:Graph = __.toyGraph
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let actual = g.Nodes
                         |> Seq.collect (fun n -> n.Ids)
                         |> Seq.map (fun id -> match id.AddressCase with    
                                               | AddressBlock.AddressOneofCase.Nodeid -> Some(id.Nodeid.Nodeid)
                                               | _ -> None
                                               )  
                         |> Seq.filter (fun x -> x.IsSome)
                         |> Seq.map (fun x -> x.Value)        
                         |> Seq.sort
                         
         output.WriteLine("loadedIds: {0}", actual |> String.concat " ")
                                       
         let expectedIds = seq { 1 .. 12 }
                           |> Seq.map (fun n -> n.ToString())
                           |> Seq.sort 
                           
         Assert.Equal<string>(expectedIds,actual)                             

    member __.CollectValues key (graph:Graph) =
        graph.Nodes
             |> Seq.collect (fun n -> n.Attributes 
                                      |> Seq.filter (fun attr -> match attr.Key.Data.DataCase with 
                                                                 | DataBlock.DataOneofCase.Binary when 
                                                                    attr.Key.Data.Binary.BinaryCase = BinaryBlock.BinaryOneofCase.Metabytes -> 
                                                                        ( key , Encoding.UTF8.GetString (attr.Key.Data.Binary.Metabytes.Bytes.ToByteArray())) 
                                                                        |> String.Equals 
                                                                 | _ -> false
                                                    )
                                      |> Seq.map (fun attr -> 
                                                    let _id = n.Ids 
                                                                |> Seq.head 
                                                                |> (fun id -> match id.AddressCase with    
                                                                              | AddressBlock.AddressOneofCase.Nodeid -> id.Nodeid.Nodeid
                                                                              | _ -> String.Empty
                                                                              )  
                                                                                                          
                                                    _id,key,attr.Value |> List.ofSeq
                                                 )                                                           
                             )
             |> List.ofSeq
             
    [<Fact>] 
    member __.``Can get labelV after load tinkerpop-crew.xml into graph`` () =
         let g:Graph = __.toyGraph
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let attrName = "labelV"
         let actual = __.CollectValues attrName g
                                       
         let expected = [ 
                                "1",attrName ,[TMDAuto <| DBBString "person"]
                                "2",attrName ,[TMDAuto <| DBBString "person"]
                                "3",attrName ,[TMDAuto <| DBBString "software"]
                                "4",attrName ,[TMDAuto <| DBBString "person"]
                                "5",attrName ,[TMDAuto <| DBBString "software"]
                                "6",attrName ,[TMDAuto <| DBBString "person"] 
                           ]
                           
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)                           
         Assert.Equal<string * string * list<TMD>>(expected,actual) 
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Age has meta type int and comes out as int`` () =
         let g:Graph = __.toyGraph
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         let attrName = "age"
         let actual = __.CollectValues attrName g                                         

         let expected = [ 
                        "1",attrName, [TMDAuto <| DBBInt 29]
                        "2",attrName, [TMDAuto <| DBBInt 27]
                        "4",attrName, [TMDAuto <| DBBInt 32]
                        "6",attrName, [TMDAuto <| DBBInt 35]
                        ]
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)
         Assert.Equal<string * string * list<TMD>>(expected,actual)         

    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'out.knows' Edges`` () =
        let g:Graph = __.toyGraph
              
        let attrName = "out.knows"
        let actual = __.CollectValues attrName g                                         
        
        let expected = [ 
                    "1",attrName, [TMDAuto <| DABTestId "7"]
                    "1",attrName, [TMDAuto <| DABTestId "8"]
                    ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
        
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'out.created' Edges`` () =        
        let g:Graph = __.toyGraph
        let attrName = "out.created"
        let actual = __.CollectValues attrName g                                         
        
        let expected = [ 
                     "1",attrName, [TMDAuto <| DABTestId "9"]
                     "4",attrName, [TMDAuto <| DABTestId "10"]
                     "4",attrName, [TMDAuto <| DABTestId "11"]
                     "6",attrName, [TMDAuto <| DABTestId "12"]
                     ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
    

    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'in.knows' Edges`` () =
        let g:Graph = __.toyGraph
              
        let attrName = "in.knows"
        let actual = __.CollectValues attrName g                                         
        
        let expected = [ 
                    "2",attrName, [TMDAuto <| DABTestId "7"]
                    "4",attrName, [TMDAuto <| DABTestId "8"]
                    ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
        
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'in.created' Edges`` () =        
        let sortedByNodeIdEdgeId (data: list<string * string * list<TMD>>) = 
            data 
            |> List.sortBy (fun (a,b,c) -> 
                                let h1 = c |> List.head
                                a , match h1.Data.DataCase with 
                                    | DataBlock.DataOneofCase.Address when h1.Data.Address.AddressCase = AddressBlock.AddressOneofCase.Nodeid ->
                                        h1.Data.Address.Nodeid.Nodeid
                                    | DataBlock.DataOneofCase.Address when h1.Data.Address.AddressCase = AddressBlock.AddressOneofCase.Globalnodeid ->
                                                                            h1.Data.Address.Globalnodeid.Nodeid.Nodeid                                        
                                    | _ ->  "")
        let g:Graph = __.toyGraph
        let attrName = "in.created"
        let actual = __.CollectValues attrName g
                    |> sortedByNodeIdEdgeId                                         
        
        let expected = [ 
                         "3",attrName, [TMDAuto <| DABTestId "9"]
                         "5",attrName, [TMDAuto <| DABTestId "10"]
                         "3",attrName, [TMDAuto <| DABTestId "11"]
                         "3",attrName, [TMDAuto <| DABTestId "12"]
                       ] 
                       |> sortedByNodeIdEdgeId
                     
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml has Edge-nodes`` () =        
        let g:Graph = __.toyGraph
        let attrName = "labelE"
        let actual = __.CollectValues attrName g                                       
        
        let expected = [ 
                         "7",attrName, [TMDAuto <| DBBString "knows"]
                         "8",attrName, [TMDAuto <| DBBString "knows"]
                         "9",attrName, [TMDAuto <| DBBString "created"]
                         "10",attrName, [TMDAuto <| DBBString "created"]
                         "11",attrName, [TMDAuto <| DBBString "created"]
                         "12",attrName, [TMDAuto <| DBBString "created"]
                       ] 
                     
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)         
         
    [<Fact>] 
    member __.``After load tinkerpop-crew.xml has node data`` () =        
        let g:Graph = __.toyGraph
        output.WriteLine(sprintf "%A" g.Nodes)
        Assert.NotEmpty(g.Nodes)

//    [<Fact>]
//    member __.``I can use the file api`` () =
//        let f = System.IO.File.Open("/home/austin/foo",FileMode.OpenOrCreate,FileAccess.ReadWrite)
//        f.Seek(10L, SeekOrigin.Begin)
//        let buffer = Array.zeroCreate 100
//        let doit = f.AsyncRead(buffer, 0, 100)
//        let size = doit |> Async.RunSynchronously
//        output.WriteLine("we read out size:{0}: {1}", size , Encoding.UTF8.GetString(buffer))
//        f.Close
//        
//        Assert.True(true)
        
    
      
