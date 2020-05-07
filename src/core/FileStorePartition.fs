namespace Ahghee

open Google.Protobuf
open Google.Protobuf.Collections
open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Linq
open System.Threading
open System.Threading.Tasks
open Ahghee.Grpc
open App.Metrics
open Metrics

type FileStorePartition(config:Config, i:int, cluster:IClusterServices) = 
    let tags = MetricTags([| "partition_id" |],
                          [| i.ToString() |]) 
    
    let ByteToHex (bytes:byte[]) =
        bytes |> Array.fold (fun state x-> state + sprintf "%02X" x) ""    
    let dir = match config.CreateTestingDataDirectory with 
                      | true -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,("data-"+ Path.GetRandomFileName())))
                      | false -> IO.Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory,"data"))

    let bc = System.Threading.Channels.Channel.CreateBounded<NodeIO>(1000)
        
    // TODO: Switch to PebblesDB when index gets to big
    // TODO: Are these per file, with bloom filters, or aross files in the same shard?
    let ``Index of NodeID -> MemoryPointer`` = new NodeIndex(Path.Combine(dir.FullName, if config.CreateTestingDataDirectory then Path.GetRandomFileName() else "nodeindex"+i.ToString()))
    
    let IndexNodeIds (nids:seq<NodeID>) =
        ``Index of NodeID -> MemoryPointer``.AddOrUpdateBatch nids 
                
    let IOThread =  
        let t = new ThreadStart((fun () -> 
            // TODO: If we cannot access this file, we need to mark this parition as offline, so it can be written to remotely
            // TODO: log file access failures
            
            
            let fileNameid = i 
            let fileName = Path.Combine(dir.FullName, (sprintf "ahghee.%i.tmp" i))
            let fileNamePos = Path.Combine(dir.FullName, (sprintf "ahghee.%i.pos" i))
            let stream = new IO.FileStream(fileName,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,1024*10000,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)

            // PRE-ALLOCATE the file to reduce fragmentation https://arxiv.org/pdf/cs/0502012.pdf
            let PreAllocSize = int64 (1024 * 1000 )
            if stream.Length < PreAllocSize then
                stream.SetLength(PreAllocSize)
            
            let arraybuffer = System.Buffers.ArrayPool<byte>.Shared 
            let mutable lastOpIsWrite = false
            let mutable lastPosition = 0L
            
           
            let loadLastPos () =
                // this cannot be called after we create a writer a few lines after we initially call loadLastPos
                // because we hold open the file.
                use posStream = new IO.FileStream(fileNamePos,IO.FileMode.OpenOrCreate,IO.FileAccess.ReadWrite,IO.FileShare.Read,8,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
                if posStream.Length < int64 8 then
                    posStream.SetLength(int64 8)
                use br = new BinaryReader(posStream)
                lastPosition <- br.ReadInt64()               
                stream.Seek (lastPosition, SeekOrigin.Begin) |> ignore
            loadLastPos()
            
            let posStream = new IO.FileStream(fileNamePos,IO.FileMode.Open,IO.FileAccess.Write,IO.FileShare.Read,8,IO.FileOptions.Asynchronous ||| IO.FileOptions.SequentialScan)
            use bw = new BinaryWriter(posStream)                
            
            // todo: make this async
            let writeLastPos (pos : int64) =
                bw.Seek (0, SeekOrigin.Begin) |> ignore
                bw.Write (pos)
            let mutable lastFlushPos = lastPosition
            let FLUSHWRITES () =   
                if lastOpIsWrite then
                    stream.Flush()
                    writeLastPos(lastPosition)
                    lastFlushPos <- lastPosition
            try
                let reader = bc.Reader
                let alldone = reader.Completion
                let myNoOp = NoOP()
                while alldone.IsCompleted = false do
                    let mutable nio: NodeIO = NodeIO.NoOP() 
                    if reader.TryRead(&nio) = false then
                        let nioWaitTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.QueueEmptyWaitTime, tags)
                        // sleep for now.
                        // todo: use this down time to do cleanup and compaction and other maintance tasks.
                        reader.WaitToReadAsync().AsTask()
                            |> Async.AwaitTask
                            |> Async.RunSynchronously
                            |> ignore
                        if(lastPosition <> lastFlushPos) then
                            //might as well do a flush
                            FLUSHWRITES()
                        
                        nio <- myNoOp // set NoOp so we can loop and check alldone.IsCompleted again.
                        nioWaitTimer.Dispose()
                        
                        
                    match nio with
                    | Add(tcs,items) -> 
                        try
                            use writeTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.AddTimer, tags)
                            // Console.WriteLine("Adding to shard "+ fileNameid.ToString())
                            if (lastOpIsWrite = false) then
                                stream.Position <- lastPosition
                                lastOpIsWrite <- true
                            let startPos = lastPosition
                            let mutable count = 0L
                            let mutable batchLen = 0L
                            let mutable ownOffset = uint64 startPos
                            
                            for item in items do
                                if  item.Id.Pointer = null then
                                    item.Id.Pointer <- Utils.NullMemoryPointer()
                                let mp = item.Id.Pointer
                                mp.Partitionkey <- uint32 i
                                mp.Filename <- uint32 fileNameid
                                mp.Offset <- ownOffset
                                mp.Length <- (item.CalculateSize() |> uint64)
                                batchLen <- batchLen + int64 mp.Length
                                ownOffset <- ownOffset + mp.Length
                                count <- count + 1L                         
                                
                            let rentedBuffer = arraybuffer.Rent(int batchLen) 
                            let out = new CodedOutputStream(rentedBuffer)
                            for item in items do        
                                item.WriteTo out
                            
                            let copyTask = stream.WriteAsync(rentedBuffer,0,int out.Position)
                                                            
                            items 
                                |> Seq.map (fun x -> x.Id) 
                                |> (fun stuff -> IndexNodeIds stuff)

                            lastPosition <- int64 ownOffset
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddFragmentMeter, tags, count)
                            config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.AddSize, tags, lastPosition - startPos)
                            config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.AddSizeBytes, tags, lastPosition - startPos)
                            copyTask |> Async.AwaitTask |> Async.RunSynchronously
                            arraybuffer.Return rentedBuffer
                            tcs.SetResult()
                            
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | Read (tcs,requests) ->  
                        try 
                            let ReadTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.ReadTimer, tags)
                            //Console.WriteLine("Reading from shard "+ fileNameid.ToString())
                            FLUSHWRITES()                          
                            lastOpIsWrite <- false
                            if requests.Length > 0 then
                                let batchStart = requests.[0].Offset
                                let batchEnd = requests.[requests.Length - 1].Offset + requests.[requests.Length - 1].Length 
                                let bufferLen = (batchEnd - batchStart)
                                let buffer = arraybuffer.Rent(int bufferLen)
                                
                                let readIOTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.ReadIOTimer, tags)
                                if stream.Position <> int64 batchStart then
                                    stream.Position <- int64 batchStart //TODO: make sure the offset is less than the end of the file
                                
                                // read a buffer that spans all of the data for every requested pointer
                                // TODO: this doesn't work for large files, or multiple files. Be smarter.
                                let readResult = stream.Read(buffer,0,int bufferLen)
                                readIOTimer.Dispose()
                                
                                if(readResult <> int bufferLen ) then
                                    raise (new Exception("wtf"))
                                if(buffer.[0] = byte 0) then
                                    raise (new Exception(sprintf "Read null data @ %A - %A\n%A" fileName requests buffer))    
                                // for every pointer requested to be read, merge them all into their own nodes
                                let outNodes = 
                                    requests
                                    |> Array.map(fun req ->
                                        let node = new Node()
                                        MessageExtensions.MergeFrom(node,buffer,int (req.Offset - requests.[0].Offset),int req.Length) 
                                        node
                                    )
                                
                                arraybuffer.Return(buffer, true) // todo: does this need to be in a finally block?
                                config.Metrics.Measure.Meter.Mark(Metrics.PartitionMetrics.ReadSize, tags, int64 bufferLen)
                                config.Metrics.Measure.Histogram.Update(Metrics.PartitionMetrics.ReadNodeFragmentCount, tags, int64 outNodes.Length)
                                tcs.SetResult(outNodes)
                                
                            else
                                tcs.SetResult(array.Empty<Node>())    
                            ReadTimer.Dispose()
                        with 
                        | ex -> 
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)
                    | FlushFixPointers (tcs) ->
                            tcs.SetResult()  
                    | FlushAdds (tcs) ->
                        try 
                            use FlushAddsTimer = config.Metrics.Measure.Timer.Time(Metrics.PartitionMetrics.FlushAddsTimer, tags)
                            FLUSHWRITES()
                            tcs.SetResult()
                        with 
                        | ex ->
                            config.log <| sprintf "ERROR[%A]: %A" i ex
                            tcs.SetException(ex)      
                    | FlushFragmentLinks (tcs) ->
                        tcs.SetResult()   
                    | NoOP(u) -> u
                    | NoOP(u) -> u
                                                                      
            finally
                config.log <| sprintf "Shutting down partition writer[%A]" i 
                config.log <| sprintf "Flushing partition writer[%A]" i
                FLUSHWRITES()
                stream.Dispose()
                (``Index of NodeID -> MemoryPointer`` :> IDisposable).Dispose()
                config.log <| sprintf "Shutting down partition writer[%A] :: Success" i                     
            ()))
        
        let thread = new Thread(t)
        thread.Start()          
        thread        
        
    // TODO: don't expose this    
    member __.Thread() = IOThread
    // TODO: don't directly expose the blocking collection. Replace with functions they can use, that way we can replace the implementation without affecting the caller
    member __.IORequests() = bc  
    // NOTE: This is allowing access to our index by other threads
    member __.Index() = ``Index of NodeID -> MemoryPointer``
