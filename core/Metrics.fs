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
    type PartitionMetrics private () =
        static let ContextName = "Partition"
        static member WriteMeter = 
            new MeterOptions(
                Context=ContextName,
                Name="Writes",
                MeasurementUnit=Unit.Calls 
            )
    
        static member WriteFragmentMeter = 
            new MeterOptions(
                Context=ContextName,
                Name="WriteFragments",
                MeasurementUnit=Unit.Items
                )
            
        static member WriteTimer = 
            new TimerOptions(
                Context=ContextName,
                Name="WriteTimer",
                MeasurementUnit=Unit.Calls,
                DurationUnit=TimeUnit.Milliseconds,
                RateUnit=TimeUnit.Milliseconds
                ) 
        
        static member WriteSize =
            new HistogramOptions(
                Context=ContextName,
                Name="WriteSize", 
                MeasurementUnit=Unit.Bytes,
                Reservoir=(fun () -> 
                                let res = new DefaultSlidingWindowReservoir(1028) 
                                res :> IReservoir)   
                )                          