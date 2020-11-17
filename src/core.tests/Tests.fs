module Tests

open System
open System.Threading
open Ekati.Core
open Ekati.TinkerPop
open FlatBuffers
open Xunit
open Xunit.Abstractions
open Ekati
open App.Metrics
open Ekati.Ext
open System.IO
open System.Text
open System.Threading.Tasks
open FSharp.Data
open RocksDbSharp
open cli

open Ekati.Ext
type StorageType =
    | Memory
    | GrpcFile

let dbtype str =
    match str with 
    | "mem" -> Memory
    | "file" -> GrpcFile
    | _ -> raise (new NotImplementedException(str + " is not a storagetype"))   

type MyTests(output:ITestOutputHelper) =
    
    let testConfig () = 
        {
        Config.ParitionCount=3; 
        log = (fun msg -> output.WriteLine msg)
        CreateTestingDataDirectory=true
        Metrics = AppMetrics
                      .CreateDefaultBuilder()
                      .Build()
        }
    
    let mkNode buf nid items =
        let map = Map.CreateMap( buf,  Map.CreateItemsVector( buf, items))
        let node = Ekati.Core.Node.CreateNode( buf,  nid,  map)
        Ekati.Core.Node.FinishNodeBuffer(buf, node)
        let bnode = Ekati.Core.Node.GetRootAsNode(buf.DataBuffer)
        bnode
        
//    member __.buildGraph (storageType:StorageType): IStorage =
//        let g:IStorage = 
//            match storageType with 
//            | GrpcFile -> new GrpcFileStore(testConfig()) :> IStorage
//        
//        let nodes = __.buildNodes
//        let task = g.Add nodes
//        match task.Status with
//        | TaskStatus.Created -> task.Start()
//        | _ -> ()                                                                     
//        task.Wait()
//        g.Flush()
//        g
        
    member __.toyGraph (storageType:StorageType): IStorage =
        let g = 
            match storageType with 
            | GrpcFile -> new GrpcFileStore(testConfig()) :> IStorage
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        match task.Status with
             | TaskStatus.Created -> task.Start()
             | _ -> ()                                                                     
        task.Wait()
        g.Flush()
        g

    [<Fact>]
    member __.``Can Dispose Faster KV``()=
        let dir = IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("index-"+ Path.GetRandomFileName())))
        let index = new NodeStore(Path.Combine( dir.FullName, "node-" ))
        index.Dispose()
    
    
    
    [<Fact>]
    member __.``Can Put KV in Faster KV``()=
        let dir = IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("index-"+ Path.GetRandomFileName())))
        let index = new NodeStore(Path.Combine( dir.FullName, "node-" ))
        let buf = FlatBufferBuilder(128)
        let now = DateTime.Now.Ticks
        let nid = ABtoyId buf "1"
        let items = [|(Utils.PropString (buf,  "someKey",  "someValue", now))|]
        let bnode = mkNode buf nid items
        let mutable bid = bnode.Id.Value
        index.RMW(ref bid, ref bnode )
        let mutable def = new Node()
        let mutable outer = new Node()

        index.Read(&bid, &def, &outer )
        Assert.NotNull(outer.ByteBuffer)
        Assert.True ( outer.Id.HasValue )
        Assert.Equal ( outer.Id.Value.Iri,  "1")
        
        index.Dispose()

    [<Fact>]
    member __.``Can Put multiple KV in Faster KV``()=
        let dir = IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("index-"+ Path.GetRandomFileName())))
        use index = new NodeStore(Path.Combine( dir.FullName, "node-" ))
        let count = 10
        for id in [| 1 .. 10 |] do
            output.WriteLine <| sprintf "putting -> "
            let mutable nnid = new NodeID()
            for offset in [| 1 .. count |] do
                let buf = FlatBufferBuilder(128)
                let now = DateTime.Now.Ticks
                let nid = ABtoyId buf <| id.ToString()
                let items = [| (Utils.PropString (buf,  ("someKey " + offset.ToString()),  ("someValue " + offset.ToString()), now)) |]
                let mutable bnode = mkNode buf nid items
                nnid <- bnode.Id.Value
                index.RMW(&nnid, &bnode )

            let mutable outt = new Node()
            Assert.True(index.TryGetValue(nnid, &outt ))
            output.WriteLine <| sprintf "Node: %A" nnid.Iri
            for p in outt.Attributes.Value.AsEnumerable() do
                output.WriteLine <| sprintf "%A %A" (p.Key.Value.Data.Value.Item<Primitive>().Value.Str) (p.Value.Value.Data.Value.Item<Primitive>().Value.Str)
                
//
//    [<Fact>]
//    member __.``Can Put dedupe KV in Faster KV``()=
//        let dir = IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("index-"+ Path.GetRandomFileName())))
//        use index = new NodeStore(Path.Combine( dir.FullName, "node-" ))
//        let count = 10
//        for id in [| 1 .. count |] do
//            
//            for offset in [| 1 .. count |] do
//                let key = ABtoyId <| id.ToString()
//                key.Pointer.Offset <- uint64 offset
//                key.Pointer.Length <- uint64 offset
//                let value = key.Pointer
//                index.RMW(ref key, ref value )
//        
//            for offset in [| 1 .. count |] do
//                let key = ABtoyId <| id.ToString()
//                key.Pointer.Offset <- uint64 offset
//                key.Pointer.Length <- uint64 offset
//                let value = key.Pointer
//                index.RMW(ref key, ref value )
//                
//            for offset in [| 1 .. count |] do
//                let key = ABtoyId <| id.ToString()
//                key.Pointer.Offset <- uint64 offset
//                key.Pointer.Length <- uint64 offset
//                let value = key.Pointer
//                index.RMW(ref key, ref value )    
//            
//            let key = ABtoyId <| id.ToString()
//            let def = NullMemoryPointer()
////            new MemoryPointer()
////            def.Offset <- uint64 12345
//            let out = new Pointers()
//            index.Read(ref key, ref def, ref out )
//        
//            Assert.InRange ( out.Pointers_.Count , count , count)
//    
//    
//    [<Fact>]
//    member __.``Can create an InternalIRI type`` () =
//        let id = DBA ( ABtoyId "1" ) 
//        let success = match id.DataCase with 
//                        | DataBlock.DataOneofCase.Nodeid -> true
//                        | _ -> false 
//        Assert.True(success)  
//        
//    [<Fact>]
//    member __.``Can create a Pair`` () =
//        let pair = PropString "firstName" "Richard"
//        let md = pair.Key.Data
//        let success = match md.DataCase with
//                        | DataBlock.DataOneofCase.Str when md.Str = "firstName" -> true 
//                        | _ -> false
//        Assert.True success     
//        
//    [<Fact>]
//    member __.``Can create a Node`` () =
//        let node = Node (ABtoyId "1") 
//                        [| PropString "firstName" "Richard" |]
//    
//        let empty = node.Attributes |> Seq.isEmpty
//        Assert.True (not empty)
//    
//    
//    
//    member __.buildNodes : seq<Node> = 
//        let node1 = Node (ABtoyId "1" ) 
//                         [|
//                            PropString "firstName" "Richard" 
//                            PropData "follows" (DABtoyId "2")
//                         |]
//                   
//        let node2 = Node (ABtoyId "2") 
//                         [|
//                            PropString "firstName" "Sam"
//                            PropData "follows" (DABtoyId "1" )
//                         |]
//                   
//        let node3 = Node (ABtoyId "3") 
//                         [|
//                            PropString "firstName" "Jim"
//                            PropData "follows" (DABtoyId "1")
//                            PropData "follows" (DABtoyId "2") 
//                         |]
//                                      
//        [| node1; node2; node3 |]      
//        |> Array.toSeq             
//    
//    member __.buildLotsNodes nodeCount perNodeFollowsCount : seq<Node> =
//        // static seed, keeps runs comparable
//        
//        let seededRandom = new Random(nodeCount)
//               
//        seq { for i in 1 .. nodeCount do 
//              yield Node (ABtoyId (i.ToString()) )
//                         ([|
//                            PropString "firstName" ("Austin")
//                            PropString "lastName"  ("Harris")
//                            PropString "age"       ("36")
//                            PropString "city"      ("Boulder")
//                            PropString "state"     ("Colorado")
//                         |] |> Seq.append (seq {for j in 0 .. perNodeFollowsCount do 
//                                                yield PropData "follows" (DABtoyId (seededRandom.Next(nodeCount).ToString()))                                                    
//                                                })
//                         
//                         )    
//            }
//    
    [<Theory>]
    [<InlineData("StorageType.GrpcFile")>]
    member __.``Can Add nodes to graph`` (storeType) =
        let g = 
            match storeType with 
            | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
            | _ -> raise <| new NotImplementedException()                                                                    
            
        let nodes = buildNodesTheCrew |> List.ofSeq
        let task = g.Add nodes
        task.Wait(TimeSpan.FromSeconds(5.0)) |> ignore
        output.WriteLine <| sprintf "task is: %A" task.Status
        g.Flush()
        g.Stop()
        output.WriteLine <| sprintf "task is now : %A" task.Status
        Assert.Equal( TaskStatus.RanToCompletion, task.Status)
        Assert.Equal( task.IsCompletedSuccessfully, true)
        ()
  
        
    [<Theory>]
    [<InlineData("StorageType.GrpcFile")>]
    member __.``Can traverse local graph index`` storeType=
        let g = 
            match storeType with 
            | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
            | _ -> raise <| new NotImplementedException() 
            
        let nodes = buildNodesTheCrew
        let task = g.Add nodes
        task.Wait()   
        g.Flush()
        let ns = g.Nodes() |> List.ofSeq
        let nodesWithIncomingEdges = ns 
                                         |> Seq.collect (fun n -> n.Attributes.Value.AsEnumerable()) 
                                         |> Seq.map (fun y -> 
                                                            match y.Value.Value.Data.Value.ItemType with  
                                                            | DataBlock.NodeID -> y.Value.Value.Data.Value.Item<NodeID>() 
                                                            | _ -> Nullable<NodeID>())   
                                         |> Seq.filter (fun x -> x.HasValue)
                                         |> Seq.map    (fun x -> x.Value )
                                         |> Seq.distinctBy (fun x -> x.Iri)
                                         |> List.ofSeq
        let loadEm =      nodesWithIncomingEdges                                    
                                         |> (fun ids -> g.Items(ids))
                                         |> Async.AwaitTask
                                         |> Async.RunSynchronously
                                         |> List.ofSeq

        let sb = new StringBuilder()
        Printers.ResultsPrinter(sb, loadEm)
            |> ignore
            
        output.WriteLine <| sb.ToString() 
        
        Assert.NotEmpty nodesWithIncomingEdges
//    
//    [<Theory>]
//    [<InlineData("StorageType.Memory")>]
//    [<InlineData("StorageType.GrpcFile")>] 
//    member __.``Can get IDs after load tinkercrew into graph`` storeType =
//         let g = 
//             match storeType with 
//             | "StorageType.Memory" ->   new MemoryStore() :> IStorage
//             | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
//             | _ -> raise <| new NotImplementedException() 
//                  
//         output.WriteLine("g.Nodes length: {0}", g.Nodes() |> Seq.length )
//         
//         let nodes = buildNodesTheCrew |> List.ofSeq
//         let task = g.Add nodes
//         task.Wait()
//         g.Flush()
//         let n1 = g.Nodes() |> List.ofSeq
//         
//         let actual = n1
//                         |> Seq.map (fun id -> 
//                                               let headId = id.Id
//                                               Some(headId.Iri)
//                                               )  
//                         |> Seq.filter (fun x -> x.IsSome)
//                         |> Seq.map (fun x -> x.Value)        
//                         |> Seq.sort
//                         |> List.ofSeq
//                         
//         output.WriteLine("loadedIds: {0}", actual |> String.concat " ")
//         output.WriteLine(sprintf "Error?: %A" task.Exception)                                        
//         let expectedIds = seq { 1 .. 12 }
//                           |> Seq.map (fun n -> n.ToString())
//                           |> Seq.sort 
//                           |> List.ofSeq
//                           
//         Assert.Equal<string>(expectedIds,actual)                             
//
    [<Theory>]
//    [<InlineData("StorageType.Memory")>]
    [<InlineData("StorageType.GrpcFile")>] 
    member __.``When I put a node in I can get the same out`` storeType =
         let g = 
             match storeType with 
             //| "StorageType.Memory" ->   new MemoryStore() :> IStorage
             | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
             | _ -> raise <| new NotImplementedException() 
                  
         
         
         let nodes = buildNodesTheCrew |> List.ofSeq |> List.sortBy (fun x -> x.Id.Value.Iri)
         let task = g.Add nodes 
         task.Wait()
         g.Flush()
         let n1 = g.Nodes() |> List.ofSeq |> List.sortBy (fun x -> x.Id.Value.Iri)
         output.WriteLine(sprintf "node in: %A" nodes )
         output.WriteLine(sprintf "node out: %A" n1 )
         Assert.Equal<Node>(nodes,n1)
// 
//    [<Theory>]
//    [<InlineData("StorageType.GrpcFile", 1)>]
//    [<InlineData("StorageType.GrpcFile", 3)>]
//    [<InlineData("StorageType.GrpcFile", 5)>] 
//    [<InlineData("StorageType.GrpcFile", 7)>] 
//    member __.``Query * Limit limits`` storeType limit =
//         let g = 
//             match storeType with 
//             | "StorageType.Memory" ->   new MemoryStore() :> IStorage
//             | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
//             | _ -> raise <| new NotImplementedException() 
//                  
//         let nodes = buildNodesTheCrew |> List.ofSeq |> List.sortBy (fun x -> x.Id.Iri)
//         let task = g.Add nodes 
//         task.Wait()
//         g.Flush()
//        
//         let query = new Step()
//         query.Limit <- new LimitFilter()
//         query.Limit.Value <- limit
//         
//         let result = g.Items([ABtoyId "*"] , query )
//                    |> Async.AwaitTask
//                    |> Async.RunSynchronously
//                    |> Seq.toList
//                    
//         Assert.InRange ( result.Length, limit , limit)
//
//    [<Theory>]
//    [<InlineData("StorageType.GrpcFile", 0)>]
//    [<InlineData("StorageType.GrpcFile", 1)>]
//    [<InlineData("StorageType.GrpcFile", 3)>]
//    [<InlineData("StorageType.GrpcFile", 5)>] 
//    [<InlineData("StorageType.GrpcFile", 7)>] 
//    member __.``Query * Skip skips`` storeType skip =
//         let g = 
//             match storeType with 
//             | "StorageType.Memory" ->   new MemoryStore() :> IStorage
//             | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
//             | _ -> raise <| new NotImplementedException() 
//                  
//         let nodes = buildNodesTheCrew |> List.ofSeq |> List.sortBy (fun x -> x.Id.Iri)
//         let task = g.Add nodes 
//         task.Wait()
//         g.Flush()
//        
//         let baseQuery = new Step()
//         baseQuery.Limit <- new LimitFilter()
//         baseQuery.Limit.Value <- 1
//         
//         let query = new Step()
//         query.Skip <- new SkipFilter()
//         query.Skip.Value <- skip
//         
//         let baseresult =
//                    g.Items([ABtoyId "*"] , baseQuery )
//                    |> Async.AwaitTask
//                    |> Async.RunSynchronously
//                    |> Seq.toList
//         
//         let result =
//                    g.Items([ABtoyId "*"] , query )
//                    |> Async.AwaitTask
//                    |> Async.RunSynchronously
//                    |> Seq.toList
//      
//         let get item =
//                        item
//                        |> List.map (fun struct(i,j) -> j)
//                        |> List.head
//      
//         let baseItem = get baseresult
//         let otherItem = get result
//         
//         if skip = 0 then
//             Assert.Equal ( baseItem , otherItem )|> ignore
//         else
//             Assert.NotEqual ( baseItem , otherItem )|> ignore
//
//    [<Theory>]
//    [<InlineData("StorageType.GrpcFile", 0, 2)>]
//    [<InlineData("StorageType.GrpcFile", 1, 2)>]
//    [<InlineData("StorageType.GrpcFile", 2, 2)>]
//    [<InlineData("StorageType.GrpcFile", 3, 2)>] 
//    [<InlineData("StorageType.GrpcFile", 4, 2)>] 
//    member __.``Query * Skip Take skips and Takes`` storeType skip take=
//         let g = 
//             match storeType with 
//             | "StorageType.Memory" ->   new MemoryStore() :> IStorage
//             | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
//             | _ -> raise <| new NotImplementedException() 
//                  
//         let nodes = buildNodesTheCrew |> List.ofSeq |> List.sortBy (fun x -> x.Id.Iri)
//         let task = g.Add nodes 
//         task.Wait()
//         g.Flush()
//        
//         let baseQuery = new Step()
//         baseQuery.Skip <- new SkipFilter()
//         baseQuery.Skip.Value <- 0
//         baseQuery.Next <- new Step()
//         baseQuery.Next.Limit <- new LimitFilter()
//         baseQuery.Next.Limit.Value <- 2
//         
//         let query = new Step()
//         query.Skip <- new SkipFilter()
//         query.Skip.Value <- skip
//         query.Next <- new Step()
//         query.Next.Limit <- new LimitFilter()
//         query.Next.Limit.Value <- take
//         
//         let baseresult =
//                    g.Items([ABtoyId "*"] , baseQuery )
//                    |> Async.AwaitTask
//                    |> Async.RunSynchronously
//                    |> Seq.toList
//         
//         let result =
//                    g.Items([ABtoyId "*"] , query )
//                    |> Async.AwaitTask
//                    |> Async.RunSynchronously
//                    |> Seq.toList
//      
//         let get item =
//                        item
//                        |> List.map (fun struct(i,j) -> j)
//                        |> List.head
//      
//         let baseItem1 = get baseresult
//         let baseItem2 = get ( baseresult |> List.tail )
//         let otherItem1 = get result
//         let otherItem2 = get ( result |> List.tail)
//         
//         Assert.InRange (result.Length, take, take)
//         
//         if skip = 0 then
//             Assert.Equal ( baseItem1 , otherItem1 )|> ignore
//             Assert.Equal ( baseItem2 , otherItem2 )|> ignore
//         else
//             Assert.NotEqual ( baseItem1 , otherItem1 )|> ignore
//             Assert.NotEqual ( baseItem2 , otherItem2 )|> ignore
//
//    
//    [<Theory>]
//    //[<InlineData("StorageType.Memory")>]
//    [<InlineData("StorageType.GrpcFile")>] 
//    member __.``When I put a nodes in their values have MemoryPointers when I get them out`` storeType =
//      let g = 
//          match storeType with 
//          | "StorageType.Memory" ->   new MemoryStore() :> IStorage
//          | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
//          | _ -> raise <| new NotImplementedException() 
//               
//      let task = g.Add buildNodesTheCrew 
//      task.Wait()
//      g.Flush()
//
//      let n1 = g.Nodes() 
//                |> List.ofSeq 
//                |> List.sortBy (fun x -> x.Id.Iri)
//                |> Seq.map (fun n -> 
//                            let valuePointers = n.Attributes
//                                                |> Seq.map (fun attr -> attr.Value)
//                                                |> Seq.filter (fun tmd ->
//                                                                match  tmd.Data.DataCase with
//                                                                | DataBlock.DataOneofCase.Nodeid -> true
//                                                                | _ -> false)
//                                                |> Seq.map (fun tmd -> tmd.Data.Nodeid)
//                            n.Id, valuePointers                                                                                
//                            )
//      
//      Assert.All<NodeID * seq<NodeID>>(n1, (fun (nid,mps) -> 
//            Assert.All<NodeID>(mps, (fun mp -> Assert.NotEqual(mp.Pointer, NullMemoryPointer())))
//        ))
//
//    [<Theory>]
//    [<InlineData("StorageType.GrpcFile", 2)>]
//    [<InlineData("StorageType.GrpcFile", 3)>]
//    [<InlineData("StorageType.GrpcFile", 4)>]
//    [<InlineData("StorageType.GrpcFile", 5)>]
//    [<InlineData("StorageType.GrpcFile", 6)>]
//    [<InlineData("StorageType.GrpcFile", 7)>]
//    [<InlineData("StorageType.GrpcFile", 8)>]
//    [<InlineData("StorageType.GrpcFile", 9)>]
//    [<InlineData("StorageType.GrpcFile", 10)>]
//    member __.``Multiple calls to add for the same nodeId results in all the fragments being linked`` storeType fragments =
//        let g = 
//            match storeType with 
//            | "StorageType.Memory" ->   new MemoryStore() :> IStorage
//            | "StorageType.GrpcFile" -> new GrpcFileStore(testConfig()) :> IStorage
//            | _ -> raise <| new NotImplementedException() 
//        
//        
//        for i in 1 .. fragments do
//            let fragment = Node (ABtoyId ("TESTID") )
//                                                    ([|
//                                                       PropString (sprintf "property-%A" i) (sprintf "%A" i)
//                                                    |]) 
//            let adding = g.Add [fragment]
//            adding.Wait()
//            
//        // TODO: Bug somewhere causing us to not wait for flush to finish, so sometimes we don't get all the adding
//        g.Flush()
//        // Flushed before we try to read the nodes.    
//        
//        let allOfThem = g.Nodes() |> List.ofSeq
//        
//        for n in allOfThem do
//            output.WriteLine (sprintf "%A %A" n.Id n.Fragments)
//        
//        let countAttributes =
//                  allOfThem
//                  |> Seq.map (fun n ->
//                                   n.Attributes
//                                        |> Seq.length)
//                  |> Seq.sum
//                  
//        Assert.InRange(countAttributes, fragments, fragments)
//        
//        // Assert that all the fragments are connected.
//
//        // put them all in a list
//        // remove the first one
//        let firstOne = allOfThem.Head
//        let theRest = allOfThem.Tail
//        
//        let rec findConnectedFragments (aFragment:Node) (otherPotentialFragments:List<Node>) (collected:List<Node>) =
//            // find the ones it has fragment links to and remove them from the list.
//            let newCollected = collected |> List.append [aFragment]
//            
//            
//            let links = 
//                otherPotentialFragments 
//                |> List.except newCollected 
//                |> List.filter (fun frag ->  aFragment.Fragments.Contains(frag.Id.Pointer))
//            
//            if (links.IsEmpty) then 
//                newCollected
//            else
//                newCollected 
//                |> List.append (links |> List.collect (fun lnk -> findConnectedFragments lnk otherPotentialFragments newCollected ))
//                |> List.distinct
//            // repeat for the one we pull out of the list.
//        
//        let connectedFragments = findConnectedFragments firstOne theRest List.empty   
//        
//        Assert.All(allOfThem, (fun frag -> 
//            Assert.NotEqual(frag.Fragments.Item(0), NullMemoryPointer())
//            Assert.Contains(frag, connectedFragments)))
// 
//    member __.CollectValues key (graph:IStorage) =
//        graph.Nodes()
//             |> Seq.collect (fun n -> n.Attributes 
//                                      |> Seq.filter (fun attr -> match attr.Key.Data.DataCase with
//                                                                 | DataBlock.DataOneofCase.Metabytes when 
//                                                                    ( key , Encoding.UTF8.GetString (attr.Key.Data.Metabytes.Bytes.ToByteArray())) 
//                                                                    |> String.Equals
//                                                                    -> true
//                                                                 | DataBlock.DataOneofCase.Str when
//                                                                     ( key , attr.Key.Data.Str)
//                                                                     |> String.Equals
//                                                                     -> true
//                                                                 | _ -> false
//                                                    )
//                                      |> Seq.map (fun attr -> 
//                                                    let _id = n.Id.Iri                                           
//                                                    _id,key,attr.Value
//                                                 )                                                           
//                             )
//             |> Seq.sortBy (fun (x,_,_) -> x)
//             |> List.ofSeq
//
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>]
//    member __.``Can get labelV after load tinkercrew into graph`` db =
//         let g = __.toyGraph (dbtype db)
//                  
//         output.WriteLine("g.Nodes length: {0}", g.Nodes() |> Seq.length )
//         
//         let attrName = "labelV"
//         let actual = __.CollectValues attrName g
//         let (_,_,one) = actual |> Seq.head 
//         let time = one.Timestamp                    
//         
//         let expected = [ 
//                                "1",attrName ,(TMDTime (DBBString "person") time)
//                                "2",attrName ,(TMDTime (DBBString "person") time)
//                                "3",attrName ,(TMDTime (DBBString "software") time)
//                                "4",attrName ,(TMDTime (DBBString "person") time)
//                                "5",attrName ,(TMDTime (DBBString "software") time)
//                                "6",attrName ,(TMDTime (DBBString "person") time) 
//                           ]
//                           
//         output.WriteLine(sprintf "foundData: %A" actual)
//         output.WriteLine(sprintf "expectedData: %A" expected)                           
//         Assert.Equal<string * string * TMD>(expected,actual) 
//         
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>] 
//    member __.``After load tinkercrew Age has meta type int and comes out as int`` db =
//         let g = __.toyGraph (dbtype db)
//                  
//         output.WriteLine("g.Nodes length: {0}", g.Nodes() |> Seq.length )
//         let attrName = "age"
//         let actual = __.CollectValues attrName g                                         
//         let (_,_,one) = actual |> Seq.head 
//         let time = one.Timestamp
//         let expected = [ 
//                        "1",attrName, TMDTime (DBBInt 29) time
//                        "2",attrName, TMDTime (DBBInt 27) time
//                        "4",attrName, TMDTime (DBBInt 32) time
//                        "6",attrName, TMDTime (DBBInt 35) time
//                        ]
//         output.WriteLine(sprintf "foundData: %A" actual)
//         output.WriteLine(sprintf "expectedData: %A" expected)
//         Assert.Equal<string * string * TMD>(expected,actual)  
//         
//    [<Theory>]
//    [<InlineData("mem", 0)>]
//    [<InlineData("mem", 1)>]
//    [<InlineData("mem", 2)>]
//    [<InlineData("file", 0)>]
//    [<InlineData("file", 1)>]
//    [<InlineData("file", 2)>]
//    member __.``After load tinkercrew multiple flush do not destroy data`` db flushes =
//         let g = __.toyGraph (dbtype db)
//         
//         for i in 0 .. flushes do
//            g.Flush()
//                  
//         output.WriteLine("g.Nodes length: {0}", g.Nodes() |> Seq.length )
//         let attrName = "age"
//         let actual = __.CollectValues attrName g                                         
//         let (_,_,one) = actual |> Seq.head 
//         let time = one.Timestamp
//         let expected = [ 
//                        "1",attrName, TMDTime (DBBInt 29) time
//                        "2",attrName, TMDTime (DBBInt 27) time
//                        "4",attrName, TMDTime (DBBInt 32) time
//                        "6",attrName, TMDTime (DBBInt 35) time
//                        ]
//         output.WriteLine(sprintf "foundData: %A" actual)
//         output.WriteLine(sprintf "expectedData: %A" expected)
//         Assert.Equal<string * string * TMD>(expected,actual)                   
//
//    [<Theory>]
//    [<InlineData("mem", 0)>]
//    [<InlineData("mem", 1)>]
//    [<InlineData("mem", 2)>]
//    [<InlineData("file", 0)>]
//    [<InlineData("file", 1)>]
//    [<InlineData("file", 2)>]
//    member __.``After load tinkercrew multiple adds do not destroy data`` db flushes =
//         let g = __.toyGraph (dbtype db)
//         
//         for i in 0 .. flushes do
//            g.Flush()
//                  
//         output.WriteLine("g.Nodes length: {0}", g.Nodes() |> Seq.length )
//         let attrName = "age"
//         let actual = __.CollectValues attrName g                                         
//         let (_,_,one) = actual |> Seq.head 
//         let time = one.Timestamp
//         let expected = [ 
//                        "1",attrName, TMDTime (DBBInt 29) time
//                        "2",attrName, TMDTime (DBBInt 27) time
//                        "4",attrName, TMDTime (DBBInt 32) time
//                        "6",attrName, TMDTime (DBBInt 35) time
//                        ]
//         output.WriteLine(sprintf "foundData: %A" actual)
//         output.WriteLine(sprintf "expectedData: %A" expected)
//         Assert.Equal<string * string * TMD>(expected,actual) 
//
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>]  
//    member __.``After load tinkercrew Nodes have 'out.knows' Edges`` db =
//        let g = __.toyGraph (dbtype db)
//              
//        let attrName = "out.knows"
//        let actual = __.CollectValues attrName g                                         
//        let (_,_,one) = actual |> Seq.head 
//        let time = one.Timestamp
//        let expected = [ 
//                    "1",attrName, TMDTime (DABtoyId "7") time
//                    "1",attrName, TMDTime (DABtoyId "8") time
//                    ]
//        output.WriteLine(sprintf "foundData: %A" actual)
//        output.WriteLine(sprintf "expectedData: %A" expected)
//        Assert.Equal<string * string * TMD>(expected,actual)
//    
//    
//        
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>] 
//    member __.``After load tinkercrew Nodes have 'out.created' Edges`` db =        
//        let g = __.toyGraph (dbtype db)
//        let attrName = "out.created"
//        let actual = __.CollectValues attrName g                                         
//        let (_,_,one) = actual |> Seq.head 
//        let time = one.Timestamp        
//        let expected = [ 
//                     "1",attrName, TMDTime (DABtoyId "9") time
//                     "4",attrName, TMDTime (DABtoyId "10") time
//                     "4",attrName, TMDTime (DABtoyId "11") time
//                     "6",attrName, TMDTime (DABtoyId "12") time
//                     ]
//        output.WriteLine(sprintf "foundData: %A" actual)
//        output.WriteLine(sprintf "expectedData: %A" expected)
//        Assert.Equal<string * string * TMD>(expected,actual)
//    
//
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>] 
//    member __.``After load tinkercrew Nodes have 'in.knows' Edges`` db =
//        let g = __.toyGraph (dbtype db)
//              
//        let attrName = "in.knows"
//        let actual = __.CollectValues attrName g                                         
//        let (_,_,one) = actual |> Seq.head 
//        let time = one.Timestamp
//        let expected = [ 
//                    "2",attrName, TMDTime (DABtoyId "7") time
//                    "4",attrName, TMDTime (DABtoyId "8") time
//                    ]
//        output.WriteLine(sprintf "foundData: %A" actual)
//        output.WriteLine(sprintf "expectedData: %A" expected)
//        Assert.Equal<string * string * TMD>(expected,actual)
//        
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>] 
//    member __.``After load tinkercrew Nodes have 'in.created' Edges`` db =        
//        let sortedByNodeIdEdgeId (data: list<string * string * TMD>) = 
//            data 
//            |> List.sortBy (fun (a,b,c) -> 
//                                let h1 = c 
//                                a , h1.Data.Nodeid.Iri)
//        let g = __.toyGraph (dbtype db)
//        let attrName = "in.created"
//        let actual = __.CollectValues attrName g
//                    |> sortedByNodeIdEdgeId                                         
//        
//        let (_,_,one) = actual |> Seq.head 
//        let time = one.Timestamp
//        
//        let expected = [ 
//                         "3",attrName, TMDTime (DABtoyId "9") time
//                         "5",attrName, TMDTime (DABtoyId "10") time
//                         "3",attrName, TMDTime (DABtoyId "11") time
//                         "3",attrName, TMDTime (DABtoyId "12") time
//                       ] 
//                       |> sortedByNodeIdEdgeId
//                     
//        output.WriteLine(sprintf "foundData: %A" actual)
//        output.WriteLine(sprintf "expectedData: %A" expected)
//        Assert.Equal<string * string * TMD>(expected,actual)
//         
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>] 
//    member __.``After load tinkercrew has Edge-nodes`` db =        
//        let g = __.toyGraph (dbtype db)
//        let attrName = "labelE"
//        let actual = __.CollectValues attrName g                                       
//        let (_,_,one) = actual |> Seq.head 
//        let time = one.Timestamp        
//        let expected = [ 
//                         "7",attrName, TMDTime (DBBString "knows") time
//                         "8",attrName, TMDTime (DBBString "knows") time
//                         "9",attrName, TMDTime (DBBString "created") time
//                         "10",attrName, TMDTime (DBBString "created") time
//                         "11",attrName, TMDTime (DBBString "created") time
//                         "12",attrName, TMDTime (DBBString "created") time
//                       ] |> List.sortBy (fun (x,_,_) -> x)
//                     
//        output.WriteLine(sprintf "foundData: %A" actual)
//        output.WriteLine(sprintf "expectedData: %A" expected)
//        Assert.Equal<string * string * TMD>(expected,actual)         
//         
//    [<Theory>]
//    [<InlineData("mem")>]
//    [<InlineData("file")>] 
//    member __.``After load tinkercrew has node data`` db =        
//        let g = __.toyGraph (dbtype db)
//        output.WriteLine(sprintf "%A" (g.Nodes()))
//        Assert.NotEmpty(g.Nodes())
//    
//    [<Fact>]
//    member __.RocksDBCanWriteAndReadKeyValue () = 
//        let temp = Path.GetTempPath()
//        
//        let path = Environment.ExpandEnvironmentVariables(Path.Combine(temp, Path.GetRandomFileName()))
//        let options = (new DbOptions()).SetCreateIfMissing(true).EnableStatistics()
//        use db = RocksDb.Open(options,path)
//        let value1 = db.Get("key")
//        db.Put("key","value")
//        let value2 = db.Get("key")
//        let nullstr = db.Get("NotThere")
//        db.Remove("key")
//        Assert.Equal("value",value2)
//        ()
//
//    [<Theory>]
//    [<InlineData("file")>] 
//    member __.QueryWillNotReturnTheSameNodeMultipleTimes db = 
//        let g = __.toyGraph (dbtype db)
//
//        let step = new Step()
//        step.Follow <- new FollowOperator()
//        step.Follow.FollowAny <- new FollowOperator.Types.FollowAny()
//        step.Follow.FollowAny.Range <- new Ahghee.Grpc.Range()
//        step.Follow.FollowAny.Range.From <- 0
//        step.Follow.FollowAny.Range.To <- 6
//        let results = g.Items([| ABtoyId "1" |], step) |> Async.AwaitTask |> Async.RunSynchronously |> Seq.toList
//        
//        let set = results
//                  |> List.map (fun struct (nodeId,result) -> nodeId)
//                  |> Set.ofList
//        
//        Assert.Equal(set.Count, results |> List.length)    
//        ()
//
