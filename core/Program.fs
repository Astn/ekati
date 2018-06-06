namespace Ahghee

open System
open System.Collections.Generic
open System.IO
open System.Linq
open System.Threading.Tasks

open App.Metrics
open Ahghee.Grpc
open Ahghee.TinkerPop
open Ahghee.Utils

module Program =
    open System.Threading
    open System.Collections.Concurrent
    open System.Runtime.InteropServices
    open System.Diagnostics
    open System.Text
    open System.Linq
    open System.Diagnostics

    let exitCode = 0


    let testConfig () = 
        {
        Config.ParitionCount=5; 
        log = (fun msg -> printf "%s" msg)
        CreateTestingDataDirectory=true
        Metrics = AppMetrics
                      .CreateDefaultBuilder()
                      .Build()
        }

    let buildLotsNodes perNodeFollowsCount : seq<Node> =
        // static seed, keeps runs comparable
        let seededRandom = new Random(1337)
        let mutable seenIds = 0               
        let simpleProps = 
            [|
                PropString "firstName" [|"Austin"|]
                PropString "lastName"  [|"Harris"|]
//                PropString "age"       [|"36"|]
//                PropString "city"      [|"Boulder"|]
//                PropString "state"     [|"Colorado"|]
             |]
             
        let mkNode i =
                Node (ABtoyId (i.ToString()) )
                             (simpleProps
                              |> Seq.append (seq {for j in 0 .. perNodeFollowsCount do 
                                                    yield PropData "follows" [| DABtoyId (seededRandom.Next(seenIds).ToString()) |]                                                    
                                                    })
                             
                             )   
             
        seq {
            for ii in 0 .. Int32.MaxValue do 
            yield (mkNode ii)
            } 
        

    let benchmark count followsCount=
        let config = testConfig()
        let mutable firstWritten = false 
        let report (file:FileStream) =
            let snap = config.Metrics.Snapshot.Get()
            let root = config.Metrics :?> IMetricsRoot
            for formatter in  root.OutputMetricsFormatters do
                if formatter.MediaType.Type = "application" then 
                    use ms = new MemoryStream()
                    if firstWritten = true then 
                        let bytes = Encoding.UTF8.GetBytes(",")
                        ms.Write(bytes,0,bytes.Length)
                    else 
                        firstWritten <- true    
                    formatter.WriteAsync(ms,snap).Wait()
                    let a = ms.ToArray()
                    file.Write(a, 0, a.Length)
                    file.Flush()
                
        let g:Graph = new Graph(new GrpcFileStore(config)) 
          
        let streamingNodes = 
            (buildLotsNodes followsCount)
            |> Seq.chunkBySize 2000
        
        let enu =
            streamingNodes.GetEnumerator()
        let f = new IO.FileStream((sprintf "./report-%A-%A.%A.json" count followsCount (DateTime.Now.ToFileTime()) ) ,IO.FileMode.Create)

        let bytesOpen = Encoding.UTF8.GetBytes("[")
        f.Write(bytesOpen,0,bytesOpen.Length)
        
        let duration = TimeSpan.FromMinutes(2.5)
        
        let start = Stopwatch.StartNew()
        let reporter = 
            async{
                while  start.Elapsed < duration do 
                    report f
                    do! Async.Sleep 5000
            }
            
        Async.Start reporter        

        let mutable t1 : Task = Task.CompletedTask
        let mutable t2 : Task = Task.CompletedTask
        let mutable ct = 0
        while start.Elapsed < duration do
            ct <- ct + 1
            enu.MoveNext() |> ignore
            t1.Wait()
            if ct % 4 = 0 then
                g.Flush()
            t1 <- t2
            t2 <- g.Add enu.Current
            
        
        g.Flush()
        g.Stop()
        System.Threading.Thread.Sleep(1000)
        Async.RunSynchronously reporter
        let bytesOpen = Encoding.UTF8.GetBytes("]")
        f.Write(bytesOpen,0,bytesOpen.Length)
        f.Flush()
        f.Dispose()


    [<EntryPoint>]
    let main args =
        printf "starting benchmark"
        benchmark 1000 2
        printf "benchmark finished, run report against report-*.json"
        exitCode
