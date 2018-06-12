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
    open Google.Protobuf
    open Grpc.Core
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
        Config.ParitionCount=Environment.ProcessorCount; 
        log = (fun msg -> printf "%s\n" msg)
        CreateTestingDataDirectory=true
        Metrics = AppMetrics
                      .CreateDefaultBuilder()
                      .Build()
        }

    let buildLotsNodes perNodeFollowsCount =
        // static seed, keeps runs comparable
        let seededRandom = new Random(1337)
        let fn = PropString "firstName" [|"Austin"|]
        let ln = PropString "lastName"  [|"Harris"|]
        let follo i = PropData "follows" [| DABtoyId (seededRandom.Next(i).ToString()) |]
        
        let pregenStuff =
            seq { for i in 1 .. 2000 do 
                    yield [|
                              fn
                              ln
                              follo i
                              follo i
                              follo i
                           |]
                }
            |> Array.ofSeq    
             
        let mkNode i =
                Node (ABtoyId (i.ToString()) )
                      pregenStuff.[seededRandom.Next(2000)]          
             
        seq {
            for ii in 0 .. 2000 .. Int32.MaxValue do 
                let output = Array.zeroCreate 2000
                Parallel.For(ii,ii+2000,(fun i -> 
                        output.[i % 2000] <- mkNode i
                    )) |> ignore
                yield output
            } 
    
    let proc = Process.GetCurrentProcess()    

    let benchmark count followsCount=
        let config = testConfig()
        let mutable firstWritten = false 
        
        // gather some process info.
        let handleCount = proc.HandleCount
        
        let nonpagedSystemMemorySize64 = proc.NonpagedSystemMemorySize64
        
        let pagedSystemMemorySize64 = proc.PagedSystemMemorySize64
        let pagedMemorySize64 = proc.PagedMemorySize64
        let peakPagedMemorySize64 = proc.PeakPagedMemorySize64
        
        let privateMemorySize64 = proc.PrivateMemorySize64
        
        let virtualMemorySize64 = proc.VirtualMemorySize64
        let peakVirtualMemorySize64 = proc.PeakVirtualMemorySize64
        
        let peakWorkingSet64 = proc.PeakWorkingSet64
        let workingSet64 = proc.WorkingSet64
        
        let totalProcessorTime = proc.TotalProcessorTime
        let privilegedProcessorTime = proc.PrivilegedProcessorTime
        let userProcessorTime = proc.UserProcessorTime
        
       
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
        
        let enu =
            streamingNodes.GetEnumerator()
        
        // print out env info.
        let envFile = sprintf "./env.info"
        use ef = new IO.FileStream(envFile, IO.FileMode.Create)
        let envms1 = new MemoryStream()
        let root = (config.Metrics :?> IMetricsRoot)
        root.DefaultOutputEnvFormatter.WriteAsync(envms1,root.EnvironmentInfo).Wait()
        let envms2 = new MemoryStream()
        // add some extra goodies.
        // Processor Count
        let emitMore name (data:'a) = 
            let bytes = 
                sprintf "%s = %A\n" name data 
                |> Encoding.UTF8.GetBytes
            envms2.Write(bytes,0,bytes.Length)
        emitMore "Processor Count" Environment.ProcessorCount
        emitMore "System Page Size" Environment.SystemPageSize
        
        let a = envms1.ToArray()
        ef.Write(a, 0, a.Length)
        let b = envms2.ToArray()
        ef.Write(b, 0, b.Length)
        
        ef.Flush()    
            
        let reportFile = sprintf "./report-%A-%A.%A.json" count followsCount (DateTime.Now.ToFileTime()) 
        let f = new IO.FileStream(reportFile ,IO.FileMode.Create)

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
            if t1.IsCompleted = false then
                t1.Wait()
            if ct % 6 = 2 then
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
        reportFile
        


    [<EntryPoint>]
    let main args =
        printf "starting benchmark\n"
        let reportFile = benchmark 1000 2
        printf "benchmark finished, run report against\n"
        printf "%s" reportFile
        exitCode
