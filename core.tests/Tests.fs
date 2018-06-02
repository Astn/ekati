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
open System.Diagnostics
open System.IO
open System.Text
open System.Threading.Tasks
open FSharp.Data


type StorageType =
    | Memory
    | GrpcFile

let dbtype str =
    match str with 
    | "mem" -> Memory
    | "file" -> GrpcFile
    | _ -> raise (new NotImplementedException(str + " is not a storagetype"))   

type MyTests(output:ITestOutputHelper) =
    
    member __.buildGraph (storageType:StorageType): Graph =
        let g:Graph = 
            match storageType with 
            | Memory ->   new Graph(new MemoryStore())
            | GrpcFile -> new Graph(new GrpcFileStore({
                                                        Config.ParitionCount=4; 
                                                        log = (fun msg -> output.WriteLine msg)
                                                        CreateTestingDataDirectory=true
                                                        }))
        
        let nodes = __.buildNodes
        let task = g.Add nodes
        match task.Status with
        | TaskStatus.Created -> task.Start()
        | _ -> ()                                                                     
        task.Wait()
        g.Flush()
        g
        
    member __.toyGraph (storageType:StorageType): Graph =
        let g:Graph = 
            match storageType with 
            | Memory ->   new Graph(new MemoryStore())
            | GrpcFile -> new Graph(new GrpcFileStore({
                                                        Config.ParitionCount=4; 
                                                        log = (fun msg -> output.WriteLine msg)
                                                        CreateTestingDataDirectory=true
                                                        }))
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
        task.Wait()
        g.Flush()
        g

    [<Fact>]
    member __.``Can create an InternalIRI type`` () =
        let id = DBA ( ABtoyId "1" ) 
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
        let node = Node (ABtoyId "1") 
                        [| PropString "firstName" [|"Richard"; "Dick"|] |]
    
        let empty = node.Attributes |> Seq.isEmpty
        Assert.True (not empty)
    
    
    
    member __.buildNodes : seq<Node> = 
        let node1 = Node (ABtoyId "1" ) 
                         [|
                            PropString "firstName" [|"Richard"; "Dick"|] 
                            PropData "follows" [| DABtoyId "2" |] 
                         |]
                   
        let node2 = Node (ABtoyId "2") 
                         [|
                            PropString "firstName" [|"Sam"; "Sammy"|] 
                            PropData "follows" [| DABtoyId "1" |]
                         |]
                   
        let node3 = Node (ABtoyId "3") 
                         [|
                            PropString "firstName" [|"Jim"|]
                            PropData "follows" [| DABtoyId "1"; DABtoyId "2" |] 
                         |]
                                      
        [| node1; node2; node3 |]      
        |> Array.toSeq             
    
    member __.buildLotsNodes nodeCount perNodeFollowsCount : seq<Node> =
        // static seed, keeps runs comparable
        
        let seededRandom = new Random(nodeCount)
               
        seq { for i in 1 .. nodeCount do 
              yield Node (ABtoyId (i.ToString()) )
                         ([|
                            PropString "firstName" [|"Austin"|]
                            PropString "lastName"  [|"Harris"|]
                            PropString "age"       [|"36"|]
                            PropString "city"      [|"Boulder"|]
                            PropString "state"     [|"Colorado"|]
                         |] |> Seq.append (seq {for j in 0 .. perNodeFollowsCount do 
                                                yield PropData "follows" [| DABtoyId (seededRandom.Next(nodeCount).ToString()) |]                                                    
                                                })
                         
                         )    
            }
    
    [<Theory>]
    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>]
    member __.``Can Add nodes to graph`` (storeType) =
        let g:Graph = 
            match storeType with 
            | "StorageType.Memory" ->   new Graph(new MemoryStore())
            | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                    Config.ParitionCount=4; 
                                                                    log = (fun msg -> output.WriteLine msg)
                                                                    CreateTestingDataDirectory=true
                                                                    }))
            | _ -> raise <| new NotImplementedException()                                                                    
            
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        task.Wait()
        output.WriteLine <| sprintf "task is: %A" task.Status
        g.Flush()
        g.Stop()
        output.WriteLine <| sprintf "task is now : %A" task.Status
        Assert.Equal( TaskStatus.RanToCompletion, task.Status)
        Assert.Equal( task.IsCompletedSuccessfully, true)
        ()
                                      
    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``Can Remove nodes from graph`` db =
        let g:Graph = __.buildGraph (dbtype db)

        let n1 = g.Nodes
        let len1 = n1 |> Seq.length
        
        let toRemove = n1 
                        |> Seq.head 
                        |> (fun n -> n.Id)
        let task = g.Remove([toRemove])
        task.Wait()
        let n2 = g.Nodes
        let len2 = n2 |> Seq.length                        
        
        Assert.NotEqual<Node> (n1, n2)
        output.WriteLine("len1: {0}; len2: {1}", len1, len2)
        (len1 = len2 + 1) |> Assert.True 
    
        
    [<Theory>]
    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>]
    member __.``Can traverse local graph index`` storeType=
        let g:Graph = 
            match storeType with 
            | "StorageType.Memory" ->   new Graph(new MemoryStore())
            | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                    Config.ParitionCount=4; 
                                                                    log = (fun msg -> output.WriteLine msg)
                                                                    CreateTestingDataDirectory=true
                                                                    }))
            | _ -> raise <| new NotImplementedException() 
            
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        task.Wait()   
        g.Flush() 
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
    
    [<Theory>]
    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>] 
    member __.``Can get IDs after load tinkerpop-crew.xml into graph`` storeType =
         let g:Graph = 
             match storeType with 
             | "StorageType.Memory" ->   new Graph(new MemoryStore())
             | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                     Config.ParitionCount=4; 
                                                                     log = (fun msg -> output.WriteLine msg)
                                                                     CreateTestingDataDirectory=true
                                                                     }))
             | _ -> raise <| new NotImplementedException() 
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let nodes = buildNodesTheCrew |> List.ofSeq
         let task = g.Add nodes
         task.Wait()
         g.Flush()
         let n1 = g.Nodes |> List.ofSeq
         
         let actual = n1
                         |> Seq.map (fun id -> 
                                               let headId = id.Id  
                                               match headId.AddressCase with    
                                               | AddressBlock.AddressOneofCase.Nodeid -> Some(headId.Nodeid.Nodeid)
                                               | _ -> None
                                               )  
                         |> Seq.filter (fun x -> x.IsSome)
                         |> Seq.map (fun x -> x.Value)        
                         |> Seq.sort
                         |> List.ofSeq
                         
         output.WriteLine("loadedIds: {0}", actual |> String.concat " ")
         output.WriteLine(sprintf "Error?: %A" task.Exception)                                        
         let expectedIds = seq { 1 .. 12 }
                           |> Seq.map (fun n -> n.ToString())
                           |> Seq.sort 
                           |> List.ofSeq
                           
         Assert.Equal<string>(expectedIds,actual)                             

    [<Theory>]
    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>] 
    member __.``When I put a node in I can get the same out`` storeType =
         let g:Graph = 
             match storeType with 
             | "StorageType.Memory" ->   new Graph(new MemoryStore())
             | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                     Config.ParitionCount=4; 
                                                                     log = (fun msg -> output.WriteLine msg)
                                                                     CreateTestingDataDirectory=true
                                                                     }))
             | _ -> raise <| new NotImplementedException() 
                  
         
         
         let nodes = buildNodesTheCrew |> List.ofSeq |> List.sortBy (fun x -> x.Id.Nodeid.Nodeid)
         let task = g.Add nodes 
         task.Wait()
         g.Flush()
         let n1 = g.Nodes |> List.ofSeq |> List.sortBy (fun x -> x.Id.Nodeid.Nodeid)
         output.WriteLine(sprintf "node in: %A" nodes )
         output.WriteLine(sprintf "node out: %A" n1 )
         Assert.Equal<Node>(nodes,n1)
 
    [<Theory>]
    //[<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>] 
    member __.``When I put a nodes in their values have MemoryPointers when I get them out`` storeType =
      let g:Graph = 
          match storeType with 
          | "StorageType.Memory" ->   new Graph(new MemoryStore())
          | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                  Config.ParitionCount=4; 
                                                                  log = (fun msg -> output.WriteLine msg)
                                                                  CreateTestingDataDirectory=true
                                                                  }))
          | _ -> raise <| new NotImplementedException() 
               
      let task = g.Add buildNodesTheCrew 
      task.Wait()
      g.Flush()
      System.Threading.Thread.Sleep(2000)
      let n1 = g.Nodes 
                |> List.ofSeq 
                |> List.sortBy (fun x -> x.Id.Nodeid.Nodeid)
                |> Seq.map (fun n -> 
                            let valuePointers = n.Attributes
                                                |> Seq.collect (fun attr -> attr.Value)
                                                |> Seq.filter (fun tmd ->
                                                                match  tmd.Data.DataCase with
                                                                | DataBlock.DataOneofCase.Address -> true
                                                                | _ -> false)
                                                |> Seq.map (fun tmd ->
                                                                match tmd.Data.Address.AddressCase with 
                                                                | AddressBlock.AddressOneofCase.Nodeid -> tmd.Data.Address.Nodeid
                                                                | AddressBlock.AddressOneofCase.Globalnodeid -> tmd.Data.Address.Globalnodeid.Nodeid
                                                                | _ -> raise (new Exception("Invalid address case")))
                            n.Id.Nodeid, valuePointers                                                                                
                            )
      
      Assert.All<NodeID * seq<NodeID>>(n1, (fun (nid,mps) -> 
            Assert.All<NodeID>(mps, (fun mp -> Assert.NotEqual(mp.Pointer, NullMemoryPointer())))
        ))
        
    [<Theory>]
    [<InlineData("StorageType.GrpcFile", 1000, 1)>]
    [<InlineData("StorageType.GrpcFile", 10000, 1)>]
    [<InlineData("StorageType.GrpcFile", 100000, 1)>]
    [<InlineData("StorageType.GrpcFile", 1000, 10)>]
    [<InlineData("StorageType.GrpcFile", 10000, 10)>]
    [<InlineData("StorageType.GrpcFile", 100000, 10)>]  
    member __.``We can nodes in 30 seconds`` storeType count followsCount=
        let g:Graph = 
          match storeType with 
          | "StorageType.Memory" ->   new Graph(new MemoryStore())
          | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                  Config.ParitionCount=4; 
                                                                  log = (fun msg -> output.WriteLine msg)
                                                                  CreateTestingDataDirectory=true
                                                                  }))
          | _ -> raise <| new NotImplementedException() 
        let staticNodes = (__.buildLotsNodes count followsCount) |> List.ofSeq
        for iter in  0 .. 5 do 
        
            let startTime = Stopwatch.StartNew()
            let task = g.Add staticNodes
            task.Wait()
            let stopTime = startTime.Stop()
            let startFlush = Stopwatch.StartNew()
            g.Flush()
            let stopFlush = startFlush.Stop()
            output.WriteLine(sprintf "Duration for %A nodes added: %A" count startTime.Elapsed )
            output.WriteLine(sprintf "Duration for %A nodes Pointer rewrite: %A" count startFlush.Elapsed )
            //Assert.InRange<TimeSpan>(startTime.Elapsed,TimeSpan.Zero,TimeSpan.FromSeconds(float 30)) 
 
    [<Theory>]
    [<InlineData("StorageType.GrpcFile", 2)>]
//    [<InlineData("StorageType.GrpcFile", 3)>]
//    [<InlineData("StorageType.GrpcFile", 4)>]
//    [<InlineData("StorageType.GrpcFile", 5)>]
//    [<InlineData("StorageType.GrpcFile", 6)>]
//    [<InlineData("StorageType.GrpcFile", 7)>]
//    [<InlineData("StorageType.GrpcFile", 8)>]
//    [<InlineData("StorageType.GrpcFile", 9)>]
//    [<InlineData("StorageType.GrpcFile", 10)>]
    member __.``Multiple calls to add for the same nodeId results in all the fragments being linked`` storeType fragments =
        let g:Graph = 
            match storeType with 
            | "StorageType.Memory" ->   new Graph(new MemoryStore())
            | "StorageType.GrpcFile" -> new Graph(new GrpcFileStore({
                                                                      Config.ParitionCount=4; 
                                                                      log = (fun msg -> output.WriteLine msg)
                                                                      CreateTestingDataDirectory=true
                                                                      }))
            | _ -> raise <| new NotImplementedException() 
        
        
        for i in 1 .. fragments do
            let fragment = Node (ABtoyId ("TESTID") )
                                                    ([|
                                                       PropString (sprintf "property-%A" i) [|sprintf "%A" i|]
                                                    |]) 
            let adding = g.Add [fragment]
            adding.Wait()
            g.Flush()
        // TODO: Bug somewhere causing us to not wait for flush to finish, so sometimes we don't get all the adding
        // Flushed before we try to read the nodes.    
        System.Threading.Thread.Sleep(2000)
        let allOfThem = g.Nodes |> List.ofSeq
        
        for n in allOfThem do
            output.WriteLine (sprintf "%A %A" n.Id n.Fragments)
        
        let len = allOfThem |> Seq.length
        Assert.InRange(len, fragments, fragments)
        
        // Assert that all the fragments are connected.

        // put them all in a list
        // remove the first one
        let firstOne = allOfThem.Head
        let theRest = allOfThem.Tail
        
        let rec findConnectedFragments (aFragment:Node) (otherPotentialFragments:List<Node>) =
            // find the ones it has fragment links to and remove them from the list.
            let links = otherPotentialFragments |> List.filter (fun frag ->  aFragment.Fragments.Contains(frag.Id.Nodeid.Pointer))
            let leftOvers = otherPotentialFragments |> List.except links
            
            if (leftOvers.IsEmpty || links.IsEmpty) then 
                links
            else
                links 
                |> List.append 
                    links 
                    |> List.collect (fun lnk -> findConnectedFragments lnk leftOvers )
            // repeat for the one we pull out of the list.
        
        let connectedFragments = List.append [firstOne] (findConnectedFragments firstOne theRest)    
        
        Assert.All(allOfThem, (fun frag -> 
            Assert.NotEqual(frag.Fragments.Item(0), NullMemoryPointer())
            Assert.Contains(frag, connectedFragments)))
 
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
                                                    let _id = n.Id 
                                                                |> (fun id -> match id.AddressCase with    
                                                                              | AddressBlock.AddressOneofCase.Nodeid -> id.Nodeid.Nodeid
                                                                              | _ -> String.Empty
                                                                              )  
                                                                                                          
                                                    _id,key,attr.Value |> List.ofSeq
                                                 )                                                           
                             )
             |> Seq.sortBy (fun (x,_,_) -> x)
             |> List.ofSeq

    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>]
    member __.``Can get labelV after load tinkerpop-crew.xml into graph`` db =
         let g:Graph = __.toyGraph (dbtype db)
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         
         let attrName = "labelV"
         let actual = __.CollectValues attrName g
         let (_,_,one) = actual |> Seq.head 
         let time = one.Head.Timestamp                    
         
         let expected = [ 
                                "1",attrName ,[TMDTime (DBBString "person") time]
                                "2",attrName ,[TMDTime (DBBString "person") time]
                                "3",attrName ,[TMDTime (DBBString "software") time]
                                "4",attrName ,[TMDTime (DBBString "person") time]
                                "5",attrName ,[TMDTime (DBBString "software") time]
                                "6",attrName ,[TMDTime (DBBString "person") time] 
                           ]
                           
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)                           
         Assert.Equal<string * string * list<TMD>>(expected,actual) 
         
    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``After load tinkerpop-crew.xml Age has meta type int and comes out as int`` db =
         let g:Graph = __.toyGraph (dbtype db)
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         let attrName = "age"
         let actual = __.CollectValues attrName g                                         
         let (_,_,one) = actual |> Seq.head 
         let time = one.Head.Timestamp
         let expected = [ 
                        "1",attrName, [TMDTime (DBBInt 29) time]
                        "2",attrName, [TMDTime (DBBInt 27) time]
                        "4",attrName, [TMDTime (DBBInt 32) time]
                        "6",attrName, [TMDTime (DBBInt 35) time]
                        ]
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)
         Assert.Equal<string * string * list<TMD>>(expected,actual)  
         
    [<Theory>]
    [<InlineData("mem", 0)>]
    [<InlineData("mem", 1)>]
    [<InlineData("mem", 2)>]
    [<InlineData("file", 0)>]
    [<InlineData("file", 1)>]
    [<InlineData("file", 2)>]
    member __.``After load tinkerpop-crew.xml multiple flush do not destroy data`` db flushes =
         let g:Graph = __.toyGraph (dbtype db)
         
         for i in 0 .. flushes do
            g.Flush()
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         let attrName = "age"
         let actual = __.CollectValues attrName g                                         
         let (_,_,one) = actual |> Seq.head 
         let time = one.Head.Timestamp
         let expected = [ 
                        "1",attrName, [TMDTime (DBBInt 29) time]
                        "2",attrName, [TMDTime (DBBInt 27) time]
                        "4",attrName, [TMDTime (DBBInt 32) time]
                        "6",attrName, [TMDTime (DBBInt 35) time]
                        ]
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)
         Assert.Equal<string * string * list<TMD>>(expected,actual)                   

    [<Theory>]
    [<InlineData("mem", 0)>]
    [<InlineData("mem", 1)>]
    [<InlineData("mem", 2)>]
    [<InlineData("file", 0)>]
    [<InlineData("file", 1)>]
    [<InlineData("file", 2)>]
    member __.``After load tinkerpop-crew.xml multiple adds do not destroy data`` db flushes =
         let g:Graph = __.toyGraph (dbtype db)
         
         for i in 0 .. flushes do
            g.Flush()
                  
         output.WriteLine("g.Nodes length: {0}", g.Nodes |> Seq.length )
         let attrName = "age"
         let actual = __.CollectValues attrName g                                         
         let (_,_,one) = actual |> Seq.head 
         let time = one.Head.Timestamp
         let expected = [ 
                        "1",attrName, [TMDTime (DBBInt 29) time]
                        "2",attrName, [TMDTime (DBBInt 27) time]
                        "4",attrName, [TMDTime (DBBInt 32) time]
                        "6",attrName, [TMDTime (DBBInt 35) time]
                        ]
         output.WriteLine(sprintf "foundData: %A" actual)
         output.WriteLine(sprintf "expectedData: %A" expected)
         Assert.Equal<string * string * list<TMD>>(expected,actual) 

    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>]  
    member __.``After load tinkerpop-crew.xml Nodes have 'out.knows' Edges`` db =
        let g:Graph = __.toyGraph (dbtype db)
              
        let attrName = "out.knows"
        let actual = __.CollectValues attrName g                                         
        let (_,_,one) = actual |> Seq.head 
        let time = one.Head.Timestamp
        let expected = [ 
                    "1",attrName, [TMDTime (DABtoyId "7") time]
                    "1",attrName, [TMDTime (DABtoyId "8") time]
                    ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
        
    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'out.created' Edges`` db =        
        let g:Graph = __.toyGraph (dbtype db)
        let attrName = "out.created"
        let actual = __.CollectValues attrName g                                         
        let (_,_,one) = actual |> Seq.head 
        let time = one.Head.Timestamp        
        let expected = [ 
                     "1",attrName, [TMDTime (DABtoyId "9") time]
                     "4",attrName, [TMDTime (DABtoyId "10") time]
                     "4",attrName, [TMDTime (DABtoyId "11") time]
                     "6",attrName, [TMDTime (DABtoyId "12") time]
                     ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
    

    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'in.knows' Edges`` db =
        let g:Graph = __.toyGraph (dbtype db)
              
        let attrName = "in.knows"
        let actual = __.CollectValues attrName g                                         
        let (_,_,one) = actual |> Seq.head 
        let time = one.Head.Timestamp
        let expected = [ 
                    "2",attrName, [TMDTime (DABtoyId "7") time]
                    "4",attrName, [TMDTime (DABtoyId "8") time]
                    ]
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
        
    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``After load tinkerpop-crew.xml Nodes have 'in.created' Edges`` db =        
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
        let g:Graph = __.toyGraph (dbtype db)
        let attrName = "in.created"
        let actual = __.CollectValues attrName g
                    |> sortedByNodeIdEdgeId                                         
        
        let (_,_,one) = actual |> Seq.head 
        let time = one.Head.Timestamp
        
        let expected = [ 
                         "3",attrName, [TMDTime (DABtoyId "9") time]
                         "5",attrName, [TMDTime (DABtoyId "10") time]
                         "3",attrName, [TMDTime (DABtoyId "11") time]
                         "3",attrName, [TMDTime (DABtoyId "12") time]
                       ] 
                       |> sortedByNodeIdEdgeId
                     
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)
         
    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``After load tinkerpop-crew.xml has Edge-nodes`` db =        
        let g:Graph = __.toyGraph (dbtype db)
        let attrName = "labelE"
        let actual = __.CollectValues attrName g                                       
        let (_,_,one) = actual |> Seq.head 
        let time = one.Head.Timestamp        
        let expected = [ 
                         "7",attrName, [TMDTime (DBBString "knows") time]
                         "8",attrName, [TMDTime (DBBString "knows") time]
                         "9",attrName, [TMDTime (DBBString "created") time]
                         "10",attrName, [TMDTime (DBBString "created") time]
                         "11",attrName, [TMDTime (DBBString "created") time]
                         "12",attrName, [TMDTime (DBBString "created") time]
                       ] |> List.sortBy (fun (x,_,_) -> x)
                     
        output.WriteLine(sprintf "foundData: %A" actual)
        output.WriteLine(sprintf "expectedData: %A" expected)
        Assert.Equal<string * string * list<TMD>>(expected,actual)         
         
    [<Theory>]
    [<InlineData("mem")>]
    [<InlineData("file")>] 
    member __.``After load tinkerpop-crew.xml has node data`` db =        
        let g:Graph = __.toyGraph (dbtype db)
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
        
    
      
