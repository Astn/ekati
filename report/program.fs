namespace report

open FSharp.Data
open XPlot.GoogleCharts

type JsonReport = JsonProvider<"report-example.json">

module Program =
    open System.IO
    open System
    open XPlot.GoogleCharts
    open XPlot.GoogleCharts.Configuration

    let exitCode = 0

    let htmlHead = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge" />
        <title>Ahghee Benchmark Report</title>
        <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
        <script type="text/javascript">
            google.charts.load('current', {
              packages: ["corechart"]
            });
        </script>
    </head>
    <body>    
    """
    let htmlFoot = """
    </body>
</html>
    """


    let metricGroups (context) (metric: JsonReport.Context -> 'a[]) (metricName: 'a -> string) (filter: 'a -> bool) (input: JsonReport.Root[])= 
                seq {
                    for foo in input do
                        let partition = foo.Contexts |> Seq.tryFind (fun x -> x.Context = context) 
                        if partition.IsSome then
                            for meter in metric partition.Value do
                                if filter meter then 
                                    yield foo.Timestamp, meter
                } 
                |> Seq.groupBy (fun (time,meter) -> metricName meter)
    
    let metricMeasure (measure: 'd -> decimal) data = 
        data
        |> Seq.map (fun (group, meters) -> 
                          meters |> Seq.map (fun (time, meter )-> time, measure meter))
    
    let metricLabels (labelBy: 'a -> string) (data: seq<string * seq<DateTime * 'a>>)=
        data
        |> Seq.map (fun (name, points) -> 
                        let t, data = (points |> Seq.head)
                        labelBy data)
    
    let metricTitle (labelBy: 'a -> string) (data: seq<string * seq<DateTime * 'a>>)=
                data
                |> Seq.head
                |> (fun (name, points) -> 
                            let t, data = (points |> Seq.head)
                            labelBy data)    

    [<EntryPoint>]
    let main args =
        
        let inFile = args |> Seq.head
        let inEnvFile = IO.Path.Combine(Path.GetDirectoryName(inFile),"env.info")   
        let envInfo = File.ReadAllText(IO.Path.Combine( Environment.CurrentDirectory,  inEnvFile))   
        let input = JsonReport.Load(IO.Path.Combine( Environment.CurrentDirectory,  inFile))
              
        
        let processGuageHandleCount = 
            let measure = "HandleCount"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "HandleCount") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
        
        let processGuageNonPagedSystemMemorySize = 
            let measure = "NonPagedSystemMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "NonPagedSystemMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )    

        let processGuagePagedSystemMemorySize = 
            let measure = "PagedSystemMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PagedSystemMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )                    
              
        let processGuagePagedMemorySize = 
            let measure = "PagedMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PagedMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) ) 

        let processGuagePeakPagedMemorySize = 
            let measure = "PeakPagedMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PeakPagedMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let processGuagePrivateMemorySize = 
            let measure = "PrivateMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PrivateMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )   

        let processGuageVirtualMemorySize = 
            let measure = "VirtualMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "VirtualMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )   

        let processGuagePeakVirtualMemorySize = 
            let measure = "PeakVirtualMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PeakVirtualMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let processGuagePeakWorkingSet = 
            let measure = "PeakWorkingSet"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PeakWorkingSet") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )  

        let processGuageWorkingSet = 
            let measure = "WorkingSet"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "WorkingSet") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )                                                    

        let processGuageTotalProcessorTime = 
            let measure = "TotalProcessorTime"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "TotalProcessorTime") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )

        let processGuagePrivilegedProcessorTime = 
            let measure = "PrivilegedProcessorTime"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "PrivilegedProcessorTime") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )            

        let processGuageUserProcessorTime = 
            let measure = "UserProcessorTime"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "UserProcessorTime") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )

        let processGuageGcEstimatedMemorySize = 
            let measure = "GcEstimatedMemorySize"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "GcEstimatedMemorySize") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let processGuageGcGenCount0 = 
            let measure = "GcGenCount0"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "GcGenCount0") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let processGuageGcGenCount1 = 
            let measure = "GcGenCount1"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "GcGenCount1") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) ) 

        let processGuageGcGenCount2 = 
            let measure = "GcGenCount2"
            let data = metricGroups "Process" (fun c -> c.Gauges) (fun m -> m.Name) (fun m -> m.Name.StartsWith "GcGenCount2") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.Value)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "Process"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )                       
              
        let filestoreTimerAddTimerDurationMean = 
            let measure = "Mean Duration"
            let data = metricGroups "FileStore" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "FileStore"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )
            
        let filestoreTimerAddTimerCallRateMean = 
            let measure = "Mean Call Rate"
            let data = metricGroups "FileStore" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Rate.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "FileStore"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) )

        let filestoreMeterAddFragmentsMeanRate = 
            let measure = "Mean"
            let data = metricGroups "FileStore" (fun c -> c.Meters) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddFragmentsMeter") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "FileStore"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let filestoreMeterAddFragmentsTotal = 
            let measure = "Total"
            let data = metricGroups "FileStore" (fun c -> c.Meters) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddFragmentsMeter") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> decimal meter.Count)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> "FileStore"))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )                  

        let partitionTimerNIOWaitDurationMean = 
            let measure = "Mean Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "NIOWaitTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )

        let partitionTimerAddDurationMean = 
            let measure = "Mean Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )
            
        let partitionTimerAddCallRateMean = 
            let measure = "Mean Call Rate"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Rate.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) )            

              
        let partitionMeterAddFragmentsMean = 
            let measure = "Mean"
            let data = metricGroups "Partition" (fun c -> c.Meters) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddFragmentsMeter") input
            let o = Options()
            o.isStacked <- true    
            data
            |> metricMeasure (fun meter -> meter.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )

        let partitionHistAddSizeMean = 
            let measure = "Mean"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> meter.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )

        let partitionHistAddSizeSum = 
            let measure = "Sum"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Sum)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )

        let partitionHistFFPReadSizeMean = 
            let measure = "Mean"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFixPointersReadSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let partitionHistFFPWriteSizeMean = 
            let measure = "Mean"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFixPointersWriteSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )    

        let partitionHistFFLReadSizeMean = 
            let measure = "Mean"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFragmentLinksReadSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )
            
        let partitionHistFFLWriteSizeMean = 
            let measure = "Mean"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFragmentLinksWriteSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.Unit) )

        let partitionTimerFlushAddsDurationMean = 
            let measure = "Mean Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushAddsTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )
            
        let partitionTimerFlushAddsCallRateMean = 
            let measure = "Mean Call Rate"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushAddsTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Rate.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) ) 

        let partitionTimerFlushFixPointersDurationMean = 
            let measure = "Mean Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFixPointersTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )
            
        let partitionTimerFlushFixPointersCallRateMean = 
            let measure = "Mean Call Rate"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFixPointersTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Rate.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) )

        let partitionTimerFlushFragmentLinksDurationMean = 
            let measure = "Mean Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFragmentLinksTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )
            
        let partitionTimerFlushFragmentLinksCallRateMean = 
            let measure = "Mean Call Rate"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "FlushFragmentLinksTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Rate.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) )

        let charts = 
            [
                processGuageHandleCount
                //processGuageNonPagedSystemMemorySize
                //processGuagePagedSystemMemorySize
                //processGuagePagedMemorySize
                //processGuagePeakPagedMemorySize
                //processGuagePrivateMemorySize
                processGuageVirtualMemorySize
                //processGuagePeakVirtualMemorySize
                //processGuagePeakWorkingSet
                processGuageWorkingSet
                processGuageTotalProcessorTime
                processGuagePrivilegedProcessorTime
                processGuageUserProcessorTime
                processGuageGcEstimatedMemorySize
                processGuageGcGenCount0
                processGuageGcGenCount1
                processGuageGcGenCount2
                filestoreTimerAddTimerDurationMean
                filestoreTimerAddTimerCallRateMean
                filestoreMeterAddFragmentsMeanRate
                filestoreMeterAddFragmentsTotal
                partitionTimerNIOWaitDurationMean
                partitionTimerAddDurationMean
                partitionTimerAddCallRateMean
                partitionMeterAddFragmentsMean
                partitionHistAddSizeMean
                partitionHistAddSizeSum
                partitionHistFFPReadSizeMean
                partitionHistFFPWriteSizeMean
                partitionHistFFLReadSizeMean
                partitionHistFFLWriteSizeMean
                partitionTimerFlushAddsDurationMean
                partitionTimerFlushAddsCallRateMean
                partitionTimerFlushFixPointersDurationMean
                partitionTimerFlushFixPointersCallRateMean
                partitionTimerFlushFragmentLinksDurationMean
                partitionTimerFlushFragmentLinksCallRateMean
            ]

        
        printf "%s" htmlHead
        
        printf "<pre>%s</pre>" envInfo
        for chart in charts do
            printf "%s" (chart.GetInlineHtml())
        printf "%s" htmlFoot
        exitCode