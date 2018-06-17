namespace Ahghee
open App.Metrics
open App.Metrics.Gauge
open App.Metrics.Meter
open App.Metrics.Timer

module Metrics =
    open App.Metrics.Histogram
    open App.Metrics.ReservoirSampling
    open App.Metrics.ReservoirSampling.SlidingWindow

    [<AbstractClass; Sealed>]
    type ProcessMetrics private () =
        static let ContextName = "Process"
        
        static member HandleCountGauge = 
            new GaugeOptions(
                Context=ContextName,
                Name="HandleCount",
                MeasurementUnit=Unit.Items
                   )        
        static member NonPagedSystemMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="NonPagedSystemMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )

        static member PagedSystemMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PagedSystemMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )
                        
        static member PagedMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PagedMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )                        

        static member PeakPagedMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PeakPagedMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )          
                        
        static member PrivateMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PrivateMemorySize",
                        MeasurementUnit=Unit.Bytes
                        ) 
                        
        static member VirtualMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="VirtualMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )    
                        
        static member PeakVirtualMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PeakVirtualMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )      
                        
        static member PeakWorkingSetGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PeakWorkingSet",
                        MeasurementUnit=Unit.Bytes
                        )

        static member WorkingSetGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="WorkingSet",
                        MeasurementUnit=Unit.Bytes
                        )

        static member TotalProcessorTimeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="TotalProcessorTime",
                        MeasurementUnit=Unit.None
                        )

        static member PrivilegedProcessorTimeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="PrivilegedProcessorTime",
                        MeasurementUnit=Unit.None
                        )

        static member UserProcessorTimeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="UserProcessorTime",
                        MeasurementUnit=Unit.None
                        )  
                        
        static member GcEstimatedMemorySizeGauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="GcEstimatedMemorySize",
                        MeasurementUnit=Unit.Bytes
                        )    

        static member GcGenCount0Gauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="GcGenCount0",
                        MeasurementUnit=Unit.Calls
                        )

        static member GcGenCount1Gauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="GcGenCount1",
                        MeasurementUnit=Unit.Calls
                        )

        static member GcGenCount2Gauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="GcGenCount2",
                        MeasurementUnit=Unit.Calls
                        )

        static member GcGenCount3Gauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="GcGenCount3",
                        MeasurementUnit=Unit.Calls
                        )

        static member GcGenCount4Gauge = 
                    new GaugeOptions(
                        Context=ContextName,
                        Name="GcGenCount4",
                        MeasurementUnit=Unit.Calls
                        )                                                                        
                                                                                                                                                                                    
    [<AbstractClass; Sealed>]
    type FileStoreMetrics private () =
        static let ContextName = "FileStore"
        
        // Adds
        static member AddFragmentMeter = 
            new MeterOptions(
                Context=ContextName,
                Name="AddFragmentsMeter",
                MeasurementUnit=Unit.Items,
                RateUnit=TimeUnit.Seconds
                )
            
        static member AddTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="AddTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                ) 
        
        static member AddGroupingTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="AddGroupingTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )
                
        static member AddTaskingTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="AddTaskingTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )        
        
        // Items
        static member ItemFragmentMeter = 
            new MeterOptions(
                Context=ContextName,
                Name="ItemFragmentMeter",
                MeasurementUnit=Unit.Items,
                RateUnit=TimeUnit.Seconds
                )  
        
        static member ItemTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="ItemTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )              
        
    [<AbstractClass; Sealed>]
    type PartitionMetrics private () =
        static let ContextName = "Partition"

        // Adds
        static member AddFragmentMeter = 
            new MeterOptions(
                Context=ContextName,
                Name="AddFragmentsMeter",
                MeasurementUnit=Unit.Items,
                RateUnit=TimeUnit.Seconds
                )
            
        static member AddTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="AddTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )
                
        static member QueueEmptyWaitTime = 
            new TimerOptions(
                Context=ContextName,
                Name="QueueEmptyWaitTime",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )          
        
        static member AddSize =
            new HistogramOptions(
                Context=ContextName,
                Name="AddSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                )    

        static member AddSizeBytes =
            new MeterOptions(
                Context=ContextName,
                Name="AddSizeBytes", 
                MeasurementUnit=Unit.Bytes
                ) 
        
        // Reads
        static member ReadTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="ReadTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )   
        
        static member ReadIOTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="ReadIOTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )           

        static member ReadSize =
            new MeterOptions(
                Context=ContextName,
                Name="ReadSize", 
                MeasurementUnit=Unit.Bytes 
                ) 
        
        static member ReadNodeFragmentCount =
            new HistogramOptions(
                Context=ContextName,
                Name="NodeFragmentCount", 
                MeasurementUnit=Unit.Items,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                )                       
                
        // FlushFixPointers
        static member FlushFixPointersTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="FlushFixPointersTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )   

        static member FlushFixPointersWriteSize =
            new HistogramOptions(
                Context=ContextName,
                Name="FlushFixPointersWriteSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                )
        
        static member FlushFixPointersReadSize =
            new HistogramOptions(
                Context=ContextName,
                Name="FlushFixPointersReadSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                ) 
                        
        // FlushAdds
        static member FlushAddsTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="FlushAddsTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )    
                
        // FlushFragmentLinks
        static member FlushFragmentLinksTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="FlushFragmentLinksTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Seconds
                )   

        static member FlushFragmentLinksWriteSize =
            new HistogramOptions(
                Context=ContextName,
                Name="FlushFragmentLinksWriteSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                )
        
        static member FlushFragmentLinksReadSize =
            new HistogramOptions(
                Context=ContextName,
                Name="FlushFragmentLinksReadSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                )                                                                                     