using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ekati;
using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;
using cli.antlr;
using DotNext.IO;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Messaging;
using parser;
using parser_grammer;
using Ekati.Core;
using Ekati.Ext;
using FlatBuffers;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Ekati.Protocol.Grpc;
using IMessage = DotNext.Net.Cluster.Messaging.IMessage;
using Utils = Ekati.Utils;

namespace server
{
    public class WatService : WatDbService.WatDbServiceBase, IInputChannel
    {
        private readonly ILogger<WatService> _logger;
        private readonly IStorage _db;
        private TimeSpan _procTotalProcessorTime;
        private DateTime _dateTime;
        private Process _proc;
        private IExpandableCluster _cluster;
        private readonly IMessageBus _clusterBus;
        private IInputChannel _inputChannelImplementation;
        private ShardAssignment _currentShardAssignment;
            
        public WatService(ILogger<WatService> logger, IStorage db)
        {
            _logger = logger;
            _db = db;
           // _cluster = cluster;
            //_clusterBus = clusterBus;
            //_clusterBus.AddListener(this);
            // these are used to keep track of cpu.
            _proc = Process.GetCurrentProcess();
            _procTotalProcessorTime = _proc.TotalProcessorTime;
            _dateTime = DateTime.UtcNow;
            //Startup();
        }

        public async void Startup()
        {
            var shardAssignment = await _clusterBus.LeaderRouter.SendMessageAsync(new ShardAssignment(_cluster.Members.Count*4, _cluster.Members).ToBinaryMessage(),
                (resp, token) => new ValueTask<ShardAssignment>(new ShardAssignment(resp)), CancellationToken.None);
            this._currentShardAssignment = shardAssignment;
        }


        public override async Task<GetMetricsResponse> GetMetrics(GetMetricsRequest request, ServerCallContext context)
        {
            throw new NotImplementedException();
            //return await _db.GetMetrics(request,context.CancellationToken);
            // var cpuPercent = UpdateCpuPercent();
        }

        private float UpdateCpuPercent()
        {
            var newnow = DateTime.UtcNow;
            var elapsed = (newnow - _dateTime).TotalMilliseconds;
            var newcpu = _proc.TotalProcessorTime;
            var cputime = (newcpu - _procTotalProcessorTime).TotalMilliseconds;
            var cpuPercent = Convert.ToSingle(cputime / (Environment.ProcessorCount * elapsed));
            _dateTime = newnow;
            _procTotalProcessorTime = newcpu;
            return cpuPercent;
        }

        private int ParseForNewLine(ReadOnlySpan<byte> data)
        {
            var newLinePos =0;
            for (int i = 0; i < data.Length; i++)
            {
                var c = (char) data[i];
            
                if (c == '\n' )
                {
                    newLinePos = i;   
                }
            }

            return newLinePos;
        }

        // we would like to be able to parse quickly. With Antlr there is supposed to be a way to parse quicker if we arent building a tree.
        // here is a starting point for exploring that. 
        private async Task<int> ParseNTStreamNoTree(Stream data)
        {
            ConcurrentQueue<Node> batch = new ConcurrentQueue<Node>();

            var parser = new NTRIPLESParser(ParserUtils.makeNTRIPLESStream(data));
            //parser.TrimParseTree = true;
            parser.BuildParseTree = true;
            int lastValidPosition = 0;
            parser.AddErrorListener(new ErrorListener());
            NTRIPLESParser.TripleContext cc = null;
            
            for (;; cc = parser.triple())
            {
                if (cc?.exception != null
                    //&& cc.exception.GetType() != typeof(Antlr4.Runtime.InputMismatchException)
                    //&& cc.exception.GetType() != typeof(Antlr4.Runtime.NoViableAltException)
                )
                {
                    Console.WriteLine(cc.exception.Message);
                    Console.WriteLine(
                        $"found {cc.exception.OffendingToken.Text} at Line {cc.exception.OffendingToken.Line} offset at {cc.exception.OffendingToken.StartIndex}");
                }

                if (cc != null)
                {
                    lastValidPosition = cc.Start.StartIndex;
                    
                }
                //lastValidPosition = parser.CurrentToken.StopIndex;                
                if (parser.CurrentToken.Type == TokenConstants.Eof)
                {
                    break;
                }
            }

            return lastValidPosition;
        }
        
        // Import NTriples into the database
        private async Task ParseNTriplesStream(Stream data)
        {
            ConcurrentQueue<Node> batch = new ConcurrentQueue<Node>();

            var parser = new NTRIPLESParser(ParserUtils.makeNTRIPLESStream(data));
            parser.BuildParseTree = true;

            async Task GroupAndAdd(List<Node> list)
            {
                var nodes = list.GroupBy(n => n.Id, (key, ns) =>
                {
                    var buf = new FlatBufferBuilder(128);
                    var nid = key.Value.CopyTo(buf);
                    var node = Node.CreateNode(buf, nid,
                        Map.CreateMap(buf, Map.CreateItemsVector(buf, ns.SelectMany(_n => _n.Attributes.Value.AsEnumerable())
                            .Select(kv => kv.CopyTo(buf)).ToArray())));
                    Node.FinishNodeBuffer(buf, node);
                    return Node.GetRootAsNode(buf.DataBuffer);
                });
                await _db.Add(nodes.ToList());
            }

            parser.AddParseListener(new NtriplesListener(async (node) =>
            {
                batch.Enqueue(node);

                // because we dealing with triples, we may get a bunch for the same nodeId
                // do a little grouping to reduce the amount of fragments we create
                if (batch.Count <= 600) return;
                var mine = new List<Node>();
                while (!batch.IsEmpty)
                {
                    if (batch.TryDequeue(out var nnnnnn))
                    {
                        mine.Add(nnnnnn);    
                    }
                }
                
                await GroupAndAdd(mine);
            }));

            parser.AddErrorListener(new ErrorListener());
            NTRIPLESParser.TripleContext cc = null;

            for (;; cc = parser.triple())
            {
                if (cc?.exception != null
                    //&& cc.exception.GetType() != typeof(Antlr4.Runtime.InputMismatchException)
                    //&& cc.exception.GetType() != typeof(Antlr4.Runtime.NoViableAltException)
                )
                {
                    Console.WriteLine(cc.exception.Message);
                    Console.WriteLine(
                        $"found {cc.exception.OffendingToken.Text} at Line {cc.exception.OffendingToken.Line} offset at {cc.exception.OffendingToken.StartIndex}");
                }
                
                if (parser.CurrentToken.Type == TokenConstants.Eof)
                {
                    break;
                }
            }
            // deal with the remainder of the batch.
            var mine2 = new List<Node>();
            while (!batch.IsEmpty)
            {
                if (batch.TryDequeue(out var nnnnnn))
                {
                    mine2.Add(nnnnnn);    
                }
            }
            if (mine2.Count > 0)
            {
                await GroupAndAdd(mine2);
            }
        }

        public override async Task Load(LoadFile request, IServerStreamWriter<QueryResponse> responseStream, ServerCallContext context)
        {
            if (request.Type == "nt")
            {
                Console.WriteLine($"Loading file: {request.Path}");
                try
                {
                    Stream data = null;
                    var TotalLength = 0L;
                    var TotalProgress = 0L;
                    var totalLengthDetermined = false;
                    if (!File.Exists(request.Path))
                    {
                        // TODO; download it.
                        var client = new HttpClient();
                        // var head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, request.Path));
                        // if (head.IsSuccessStatusCode)
                        // {
                        //     head.Headers.
                        // }
                        data = await client.GetStreamAsync(request.Path);
                        //TotalLength = data.Length;
                    }
                    else
                    {
                        data = File.OpenRead(request.Path);
                        TotalLength = data.Length;
                        totalLengthDetermined = true;
                    }

                    var timer = Stopwatch.StartNew();
                    await using var cleanup = data;
                    await using var bs = new BufferedStream(data, 4096);
                    {
                        var lastNewLinePosition = 0;
                        var memoryWritePos = 0L;
                        var unReadBytes = 0L;
                        var memory = new byte[81920];
                        await using var mainView = new MemoryStream(memory);
                        do
                        {
                            var newBytesAdded = await bs.ReadAsync(memory, (int)memoryWritePos, (int)(memory.Length - memoryWritePos));
                            if (!totalLengthDetermined)
                            {
                                TotalLength += newBytesAdded;
                            }
                            var newBytesEndPos = memoryWritePos + newBytesAdded;
                            if (newBytesAdded == 0)
                                break;
                            // move back to beginning
                            mainView.Seek(0, SeekOrigin.Begin);
                            
                            // find good place to stop
                            //var endOfLastTriple = await ParseNTStreamNoTree(new MemoryStream(memory, 0,(int)newBytesEndPos));
                            var endOfLastTriple = ParseForNewLine(new Span<byte>(memory, 0, (int) newBytesEndPos));
                            // just incase we have have reached the end, we want to make sure we try to read the last bit...
                            if (endOfLastTriple == 0 && newBytesEndPos > 0)
                            {
                                unReadBytes = newBytesEndPos;
                            }
                            else
                            {
                                unReadBytes = newBytesEndPos - endOfLastTriple;
                            }
                            
                            // create a vew up to that position;
                            var pageView = new MemoryStream(memory, 0, endOfLastTriple);

                            // run the parser on that pageView.
                            await ParseNTriplesStream(pageView);
                            // move the end of the stream that we didn't read back to the beginning
                            //Console.WriteLine($"BBC: endoflastTriple:{endOfLastTriple}, unReadBytes:{unReadBytes}, urb32:{Convert.ToInt32(unReadBytes)}");
                            Buffer.BlockCopy(memory, endOfLastTriple, memory, 0, Convert.ToInt32(unReadBytes));
                            memoryWritePos = unReadBytes;
                            TotalProgress += endOfLastTriple;
                            if (timer.Elapsed > TimeSpan.FromSeconds(1))
                            {
                                var qr = new QueryResponse();
                                qr.LoadFileResponse = new LoadFileResponse
                                {
                                    Progress = TotalProgress,
                                    Length = TotalLength
                                };
                                 
                                await responseStream.WriteAsync(qr);    
                            }

                            if (context.CancellationToken.IsCancellationRequested)
                            {
                                Console.WriteLine($"Cancel requested for file load. {request.Path }");
                                break;
                            }
                        } while (unReadBytes > 0);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            } else if (request.Type == "graphml")
            {
                Console.WriteLine($"Loading file: {request.Path}");
                try
                {
                    var sw = Stopwatch.StartNew();
                    var nodes = TinkerPop.buildNodesFromFile(request.Path);
                    await _db.Add(nodes).ContinueWith(adding =>
                    {
                        if (adding.IsCompletedSuccessfully)
                        {
                            sw.Stop();
                            Console.WriteLine(
                                $"\nstatus> put done in {sw.ElapsedMilliseconds}ms");
                        }
                        else
                        {
                            Console.WriteLine(
                                $"\nstatus> put err({adding?.Exception?.InnerException?.Message})");
                        }
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        public override async Task Query(QueryRequest request, IServerStreamWriter<QueryResponse> responseStream, ServerCallContext context)
        {
            var parser = new AHGHEEParser(ParserUtils.makeStream(request.QueryText));
            parser.BuildParseTree = true;
            parser.AddParseListener(listener: new Listener(async (nodes) =>
            {
                // Handle inserting of nodes via a Put command
                try
                {
                    // todo: Should we batch in chucks of these nodes?
                    foreach (var node in nodes)
                    {
                        var qr = new QueryResponse();
                        await _db.Add(new[] { node });
                        qr.PutResponse = new PutResponse { Success = true };
                        await responseStream.WriteAsync(qr);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ex {ex} stack: {ex.StackTrace}");
                }
            }, async (nids) =>
            {
                // Handle querying nodes
                var gr = new GetRequest();
                gr.Iris.AddRange(nids.Select(nid => nid.Iri));
                await Get(gr, responseStream, context);
            }, () => { },
                async (loadType, path) =>
                {
                    var lf = new LoadFile();
                    lf.Type = loadType;
                    lf.Path = path.Trim('\"');
                    await Load(lf, responseStream, context);
                }));
            parser.AddErrorListener(new ErrorListener());
            AHGHEEParser.CommandContext cc = null;

            for (; ; cc = parser.command())
            {
                if (cc?.exception != null)
                {
                    Console.WriteLine( $"{cc.exception.Message} - found {cc.exception.OffendingToken.Text} at Line {cc.exception.OffendingToken.Line} offset at {cc.exception.OffendingToken.StartIndex}");
                }

                if (parser.CurrentToken.Type == TokenConstants.Eof)
                {
                    break;
                }
            }
        }

        public override async Task Get(GetRequest request, IServerStreamWriter<QueryResponse> responseStream, ServerCallContext context)
        {
            try
            {
                // convert iri strings to flatbuffer NodeIDs
                var nodeIds = request.Iris.Select(iri =>
                {
                    var builder = new FlatBufferBuilder(16);

                    var nid = NodeID.CreateNodeID(builder, "".CopyTo(builder), iri.CopyTo(builder));
                    builder.Finish(nid.Value);
                    return NodeID.GetRootAsNodeID(builder.DataBuffer);
                });
                var result = await _db.Items(nodeIds);
                var sendCount = 0;
                foreach (var chunk in result)
                {
                    sendCount++;
                    var fn = new FlatNode();
                    // todo: remove the double copy here
                    fn.FlatBuffer = ByteString.CopyFrom(chunk.ByteBuffer.ToFullArray());
                    var qr = new QueryResponse();
                    qr.Node = fn;
                    await responseStream.WriteAsync(qr);
                }
                _logger.LogInformation($"Sent back {sendCount} items.");
            }
            catch (Exception e)
            {
                _logger.LogError(e,"Get failed");
            }
        }

        public override async Task Put(FlatNode request, IServerStreamWriter<QueryResponse> responseStream, ServerCallContext context)
        {
            var qr = new QueryResponse();
            try
            {
                // todo: remove the copy here
                var node = Node.GetRootAsNode(new ByteBuffer(request.FlatBuffer.ToByteArray()));
                await _db.Add(new[] {node});
                
                qr.PutResponse = new PutResponse { Success = true };
                await responseStream.WriteAsync(qr);
            }
            catch (Exception e)
            {
                _logger.LogError(e,"Put failed");
                qr.PutResponse = new PutResponse { Success = false };
                await responseStream.WriteAsync(qr);
            }
        }

        public override async Task<GetStatsResponse> GetStats(GetStatsRequest request, ServerCallContext context)
        {
            throw new NotImplementedException();
            // return await _db.GetStats(request, context.CancellationToken);
            
        }

        public override Task<ListStatsResponse> ListStats(ListStatsRequest request, ServerCallContext context)
        {
            return base.ListStats(request, context);
        }

        // these are messages from some other node in the cluster
        public Task<IMessage> ReceiveMessage(ISubscriber sender, IMessage message, object? context, CancellationToken token)
        {
            if (message.Name == server.ShardAssignment.Name)
            {
                if (_cluster.Leader.IsRemote)
                {
                    // shard assignment requests should always be sent to the leader, so we if get one as
                    // a follower then something went wrong
                    _logger.LogWarning("Got shard assignment message as a follower");
                }

                // if we have a current shardAssignment, and one that was sent in that are different
                // we should check membership for new servers, or missing servers.
                
                
                
                // if we have new servers, ....
                
                
                // if we have missing servers ...
                
                 
                // if we have same servers, but different assignments, then we will want to either keep it the same
                // or determine if we should balance some things around.
                
                var sa = new ShardAssignment(_cluster.Members.Count * 4, _cluster.Members);
                this._currentShardAssignment = sa;
                return Task.FromResult(sa.ToBinaryMessage() as IMessage);
            }
            throw new NotImplementedException();
        }

        // these are signals from some other node in the cluster
        public Task ReceiveSignal(ISubscriber sender, IMessage signal, object? context, CancellationToken token)
        {
            switch (signal.Name)
            {
                case "" : break;
                default: break;
            }
            throw new NotImplementedException();
        }
    }
}
