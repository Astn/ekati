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
        
        static member AddSize =
            new HistogramOptions(
                Context=ContextName,
                Name="AddSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
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

        static member ReadSize =
            new HistogramOptions(
                Context=ContextName,
                Name="ReadSize", 
                MeasurementUnit=Unit.Bytes,
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