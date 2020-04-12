namespace Ahghee

open System
open System.Collections.Generic
open System.IO
open System.Linq
open System.Threading.Tasks

open System
open System
open System.Buffers
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
        let fn = PropString "firstName" "Austin"
        let ln = PropString "lastName"  "Harris"
        let follo i = PropData "follows" ( DABtoyId (seededRandom.Next(i).ToString()) )
        
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
        
        let collectSystemMetrics() =
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.HandleCountGauge, float proc.HandleCount)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.NonPagedSystemMemorySizeGauge, float proc.NonpagedSystemMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PagedSystemMemorySizeGauge, float proc.PagedSystemMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PagedMemorySizeGauge, float proc.PagedMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PeakPagedMemorySizeGauge, float proc.PeakPagedMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PrivateMemorySizeGauge, float proc.PrivateMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.VirtualMemorySizeGauge, float proc.VirtualMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PeakVirtualMemorySizeGauge, float proc.PeakVirtualMemorySize64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PeakWorkingSetGauge, float proc.PeakWorkingSet64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.WorkingSetGauge, float proc.WorkingSet64)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.TotalProcessorTimeGauge, float proc.TotalProcessorTime.TotalMilliseconds)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.PrivilegedProcessorTimeGauge, float proc.PrivilegedProcessorTime.TotalMilliseconds)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.UserProcessorTimeGauge, float proc.UserProcessorTime.TotalMilliseconds)
            config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcEstimatedMemorySizeGauge, float (GC.GetTotalMemory(false)))
            let maxGen = GC.MaxGeneration
            if (0 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount0Gauge, float (GC.CollectionCount(0)))
            if (1 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount1Gauge, float (GC.CollectionCount(1)))
            if (2 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount2Gauge, float (GC.CollectionCount(2)))
            if (3 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount3Gauge, float (GC.CollectionCount(3)))
            if (4 <= maxGen) then
                config.Metrics.Measure.Gauge.SetValue(Metrics.ProcessMetrics.GcGenCount4Gauge, float (GC.CollectionCount(4)))        

            ()        
       
        let report (file:FileStream) =
            collectSystemMetrics()
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
                
        let g:IStorage = new GrpcFileStore(config) :> IStorage 
          
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
        let timer = Stopwatch.StartNew()
        let mutable stop = false 
        let reporter = 
            async{
                while not stop do 
                    report f
                    do! Async.Sleep 10000
                 
            }
                    
        Async.Start reporter        

        let mutable t1 : Task = Task.CompletedTask
        let mutable t2 : Task = Task.CompletedTask
        let mutable ct = 0
        while timer.Elapsed < duration do
            ct <- ct + 1
            enu.MoveNext() |> ignore
            if t1.IsCompleted = false then
                t1.Wait()
            if ct % 12 = 0 then
                g.Flush()
            t1 <- t2
            t2 <- g.Add enu.Current
            
        
        g.Flush()
        
        System.Threading.Thread.Sleep(30000)
        
        // now for the read test
        let readTimer = Stopwatch.StartNew()
        
        let mutable count = 0
        let readEnu = g.Nodes.GetEnumerator()
        
        while readTimer.Elapsed < duration do
            if readEnu.MoveNext() then
                count <- count + 1
            else
                ()
        stop <- true        
        Async.RunSynchronously reporter
        g.Stop()
        let bytesOpen = Encoding.UTF8.GetBytes("]")
        f.Write(bytesOpen,0,bytesOpen.Length)
        f.Flush()
        f.Dispose()
        reportFile
        


    [<EntryPoint>]
    let main args =
        let config =
            {
                Config.ParitionCount = Convert.ToInt32( 0.75m * Convert.ToDecimal( Environment.ProcessorCount) ); 
                log = (fun msg -> printf "%s\n" msg)
                CreateTestingDataDirectory=false
                Metrics = AppMetrics
                              .CreateDefaultBuilder()
                              .Build()
            }
        let g:IStorage = new GrpcFileStore(config) :> IStorage 
        // let reader = new StreamReader(new MemoryStream());
        // Console.SetIn reader
        let bpool = ArrayPool<char>.Create()
        while true do
            Console.Write("ahghee>");
            let b = Console.ReadLine()
            if b.StartsWith("benchmark") then
                printf "starting benchmark\n"
                let reportFile = benchmark 1000 2
                printf "benchmark finished, run report against\n"
                printf "%s" reportFile
            if b.StartsWith("put") then
                try
                     
                    
                    let mutable putmore = true
                    
                    while putmore  do
                        Console.Write("put> ")
                        let mutable line = Console.ReadLine()
                        putmore <- String.IsNullOrWhiteSpace(line) = false
                        let node = Google.Protobuf.JsonParser.Default.Parse<Node>(line)
                        node.Id.Pointer <- NullMemoryPointer()
                        node.Fragments.Add (NullMemoryPointer())
                        node.Fragments.Add (NullMemoryPointer())
                        node.Fragments.Add (NullMemoryPointer())
                            
                        let adding = g.Add ( [node] )
                        Console.WriteLine("ok> " + Google.Protobuf.JsonFormatter.Default.Format(node))
                        adding.Wait()
                with
                    e ->
                        Console.WriteLine(e.Message)
                g.Flush()
                ()
            else if b.StartsWith("getf") then
                try
                    
                    let mutable getmore = true
                    while getmore do
                        Console.Write("getf> ")
                        let mutable line = Console.ReadLine()
                        getmore <- String.IsNullOrWhiteSpace(line) = false
                        let ab = Google.Protobuf.JsonParser.Default.Parse<NodeID>(line)
                        ab.Pointer <- NullMemoryPointer()
                        Console.WriteLine()
                        g.Nodes
                            |> Seq.filter (fun n -> n.Id.Iri = ab.Iri)
                            |> Seq.iter (fun n -> Console.WriteLine("ok> " + Google.Protobuf.JsonFormatter.Default.Format(n)))
                        
                with
                    e ->
                        Console.WriteLine(e.Message)
                ()
            else if b.StartsWith("get") then
                try
                    
                    let mutable getmore = true
                    while getmore do
                        Console.Write("get> ")
                        let mutable line = Console.ReadLine()
                        getmore <- String.IsNullOrWhiteSpace(line) = false
                        let ab = Google.Protobuf.JsonParser.Default.Parse<NodeID>(line)
                        ab.Pointer <- NullMemoryPointer()
                        Console.WriteLine()
                        let t = g.Items([ab])
                        t.Result
                            |> Seq.iter (fun (a,e) -> match e with
                                                        | Left(node) -> Console.WriteLine("ok> " + Google.Protobuf.JsonFormatter.Default.Format(node))
                                                        | Right(e) -> Console.WriteLine("err> " + e.Message) )                            
                with
                    e ->
                        Console.WriteLine(e.Message)
                ()    
            else
                Console.WriteLine("unexpected input. Expected put|get")
            ()
                
        exitCode
