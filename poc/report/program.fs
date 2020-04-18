namespace report

open System.Text
open FSharp.Data
open XPlot.GoogleCharts
open System.IO
open System
open XPlot.GoogleCharts
open XPlot.GoogleCharts.Configuration
type JsonReport = JsonProvider<"report-example.json">

module Program =


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
        input
        |> Seq.collect( fun x ->  x.Contexts |> Seq.map (fun y -> x.Timestamp.DateTime, y))
        |> Seq.filter (fun (t, x) -> x.Context = context)
        |> Seq.collect (fun (t, x) -> metric x |> Seq.map (fun y -> t, y))
        |> Seq.filter (fun (t, x) -> filter x)
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

    let main (inFile:string, outSB:StringBuilder)  =
        
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
            |> Seq.collect(fun x -> x)
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
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )

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
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )
            
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
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )                  

        let partitionTimerQueueEmptyWaitTimeMean = 
            let measure = "Mean Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "QueueEmptyWaitTime") input
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
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )            

              
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
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )

        let partitionHistAddSizeMean = 
            let measure = "Mean Size Per Call"
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

        let partitionMeterAddSizeRate = 
            let measure = "Mean Rate"
            let data = metricGroups "Partition" (fun c -> c.Meters) (fun m -> m.Name) (fun m -> m.Name.StartsWith "AddSizeBytes") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%s / %s" d.Unit d.RateUnit) )
            
        let partitionMeterReadSizeRate = 
            let measure = "Mean Rate"
            let data = metricGroups "Partition" (fun c -> c.Meters) (fun m -> m.Name) (fun m -> m.Name.StartsWith "ReadSize") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.MeanRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%s / %s" d.Unit d.RateUnit) )    

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
            let measure = "Mean Size Per Call"
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
            let measure = "Mean Size Per Call"
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
            let measure = "Mean Size Per Call"
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
            let measure = "Mean Size Per Call"
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
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) ) 

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
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )

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
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )

        let partitionTimerReadCallRateMean = 
            let measure = "Mean ms Duration"
            let saw = new System.Collections.Generic.List<string>()
            let data = 
                metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> saw.Add(m.Name); m.Name.StartsWith "ReadTimer") input
                |> List.ofSeq
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) )
        
        let partitionTimerReadCallRate99th = 
            let measure = "99th ms Duration"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "ReadTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Percentile99)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.RateUnit) )

        let partitionTimerReadCallCount = 
            let measure = "Total Call Count"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "ReadTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Count)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> "Calls Total") )

        let partitionTimerReadCallCountOneMinuteRate = 
            let measure = "One Minute Rate"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "ReadTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Rate.OneMinuteRate)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> d.RateUnit) )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> sprintf "%A / %A" d.Unit d.RateUnit) )                                

        let partitionTimerReadCallNodeFragmentCountMean =  
            let measure = "Histogram 1028 window - Mean Node Fragments per call"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "NodeFragmentCount") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Mean)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> "Fragments - Per Call") )

        let partitionTimerReadCallNodeFragmentCount =  
            let measure = "Histogram 1028 window - Node Fragments Sum"
            let data = metricGroups "Partition" (fun c -> c.Histograms) (fun m -> m.Name) (fun m -> m.Name.StartsWith "NodeFragmentCount") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Sum)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> "Nodes Fragments") )
        
        let partitionTimerReadIOTimeMean =  
            let measure = "Histogram 1028 window - Read IO Time - Mean"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "ReadIOTimer") input
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

        let partitionTimerReadIOTime99th =  
            let measure = "Histogram 1028 window - Read IO Time - 99th Percentile"
            let data = metricGroups "Partition" (fun c -> c.Timers) (fun m -> m.Name) (fun m -> m.Name.StartsWith "ReadIOTimer") input
            let o = Options()
            o.isStacked <- true
            data
            |> metricMeasure (fun meter -> decimal meter.Histogram.Percentile99)
            |> Chart.SteppedArea
            |> Chart.WithLabels (data |> metricLabels (fun data -> sprintf "%A" data.Tags.PartitionId))
            |> Chart.WithOptions o
            |> Chart.WithTitle (data |> metricTitle (fun d -> sprintf "%s - %s" (d.Name.Split('|') |> Seq.head) measure ))
            |> Chart.WithXTitle (data |> metricTitle (fun d -> "Time") )
            |> Chart.WithYTitle (data |> metricTitle (fun d -> d.DurationUnit) )            
            
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
                
                partitionTimerReadCallRateMean
                partitionTimerReadCallRate99th
                partitionTimerReadCallCount
                partitionTimerReadCallCountOneMinuteRate
                partitionTimerReadCallNodeFragmentCountMean
                partitionTimerReadCallNodeFragmentCount
                partitionTimerReadIOTimeMean
                partitionTimerReadIOTime99th
                
                partitionTimerQueueEmptyWaitTimeMean
                partitionTimerAddDurationMean
                partitionTimerAddCallRateMean
                partitionMeterAddFragmentsMean
                partitionHistAddSizeMean
                partitionHistAddSizeSum
                partitionMeterAddSizeRate
                partitionMeterReadSizeRate
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

        
        outSB.Append(htmlHead).Append("\n").Append("<pre>").Append(envInfo).Append("</pre>\n") |> ignore 

        for chart in charts do
            outSB.AppendLine(chart.GetInlineHtml()) |> ignore
        outSB.AppendLine(htmlFoot) |> ignore
        exitCode