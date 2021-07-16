using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ekati;
using Ekati.Core;
using Ekati.Ext;
using benchmark;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;
using FlatBuffers;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Microsoft.FSharp.Core;
using RocksDbSharp;
using Utils = Ekati.Grpc.Utils;

namespace benchmark
{
    [MinColumn, MaxColumn]
    public class CreatingNodeEmpty
    {
        [Benchmark(Baseline = true)]
        public Node MkNodeEmpty()
        {
            var buf = new FlatBufferBuilder(32);
            var n= Ekati.Utils.Node(buf, NodeID.CreateNodeID(buf), Map.CreateMap(buf));
            Node.FinishNodeBuffer(buf,n);
            return Node.GetRootAsNode(buf.DataBuffer);
        }
    }

    [EventPipeProfiler(EventPipeProfile.CpuSampling)]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class WriteNodesBenchmark
    {
        private GrpcFileStore g;
        private IStorage istor;
        private IDisposable idis;
        private IList<Node> seedData;
        private Random rnd;
        public WriteNodesBenchmark()
        {
            var config = Ekati.Program.testConfig();
            g = new GrpcFileStore(config);
            istor = g;
            idis = g;
            seedData = Ekati.Program.buildLotsNodes(2).ToList();
            rnd = new Random(1337);
        }
        
        [Benchmark]
        public async Task AddNodes1()
        {
            var nodes = seedData.Skip(rnd.Next(seedData.Count - 1)).Take(1);
            await istor.Add(nodes);
        }
        [Benchmark]
        public async Task AddNodes10()
        {
            var nodes = seedData.Skip(rnd.Next(seedData.Count - 10)).Take(10);
            await istor.Add(nodes);
        }
        [Benchmark]
        public async Task AddNodes100()
        {
            var nodes = seedData.Skip(rnd.Next(seedData.Count - 100)).Take(100);
            await istor.Add(nodes);
        }

        private void ReleaseUnmanagedResources()
        {
            idis.Dispose();
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        ~WriteNodesBenchmark()
        {
            ReleaseUnmanagedResources();
        }
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            var AddNodesBench = BenchmarkRunner.Run<WriteNodesBenchmark>( DefaultConfig.Instance.WithOption(ConfigOptions.DisableOptimizationsValidator,true));
            //var summary4 = BenchmarkRunner.Run<WriteNodesBenchmark>();
            //var summary0 = BenchmarkRunner.Run<CreatingTypes>();
            //var summary1 = BenchmarkRunner.Run<CreatingKeyValue>();
            //var summary2 = BenchmarkRunner.Run<CreatingNodeEmpty>();
            //var summary3 = BenchmarkRunner.Run<RocksDbSinglePut>();
        }
    }
}