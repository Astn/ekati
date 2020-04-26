using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ahghee;
using Ahghee.Grpc;
using benchmark;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Microsoft.FSharp.Core;
using RocksDbSharp;
using Utils = Ahghee.Grpc.Utils;

namespace benchmark
{
    [MinColumn, MaxColumn]
    public class RocksDbSinglePut:IDisposable
    {
        private readonly RocksDb db;
        private readonly NodeIdIndex nodeIndex;
        private int ctr;
        private readonly MemoryPointer mp;
        private readonly Pointers rp;
        private readonly ConcurrentDictionary<int,Pointers> cd= new ConcurrentDictionary<int,Pointers>();
        private int idHash;
        private NodeID Nodeid;
        public RocksDbSinglePut()
        {
            var temp = Path.GetTempPath();
            var options = (new DbOptions()).SetCreateIfMissing(true).EnableStatistics();
            db = RocksDb.Open(options, Environment.ExpandEnvironmentVariables(Path.Combine(temp, Path.GetRandomFileName())));

            nodeIndex = new NodeIdIndex(Environment.ExpandEnvironmentVariables(Path.Combine(temp, Path.GetRandomFileName())));
            
            Nodeid = new NodeID();
            Nodeid.Pointer = Utils.NullMemoryPointer();
            Nodeid.Remote = "graph";
            Nodeid.Iri = "1";
            
            rp = new Pointers();
            mp = Utils.NullMemoryPointer();
            mp.Offset = 100UL;
            mp.Length = 200UL;
            rp.Pointers_.Add(mp);
           

        }
        
        [Benchmark(Baseline = true)]
        public void PutRocksDbStrings()
        {
            db.Put(ctr.ToString(), "value");
            Interlocked.Increment(ref ctr);
        }
        
        [Benchmark]
        public void PutNodeIdIndex()
        {
            Nodeid.Iri = Interlocked.Increment(ref ctr).ToString();
            var idHash = Nodeid.GetHashCode();
            nodeIndex.AddOrUpdateCS(new [] {Nodeid});   
        }
        [Benchmark]
        public void PutConcurrentDictionary()
        {
            Nodeid.Iri = Interlocked.Increment(ref ctr).ToString();
            var idHash = Nodeid.GetHashCode();
            var value = cd.AddOrUpdate(idHash, rp, (id, rp) => rp);   
        }

        private void ReleaseUnmanagedResources()
        {
            db.Dispose();
            (nodeIndex as IDisposable).Dispose();
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        ~RocksDbSinglePut()
        {
            ReleaseUnmanagedResources();
        }
    }
    
    [MinColumn, MaxColumn]
    public class CreatingNodeEmpty
    {
        [Benchmark(Baseline = true)]
        public Node MkNodeEmpty() => Ahghee.Utils.Node(new NodeID(), Array.Empty<KeyValue>());

    }

    [MinColumn, MaxColumn]
    public class CreatingKeyValue
    {
        private static string _graph = "graph1";
        private static string _id = "12345";
        private static string _key1 = "name";
        private static string _value1 = "Austin";
        private byte[] _value1Bytes;
        private byte[] _key1Bytes;
        private ByteString _key1ProtoBytes;
        private ByteString _value1ProtoBytes;

        public CreatingKeyValue()
        {
            _key1Bytes = Encoding.UTF8.GetBytes(_key1);
            _value1Bytes = Encoding.UTF8.GetBytes(_value1);
            _key1ProtoBytes = Google.Protobuf.ByteString.CopyFrom(_key1Bytes);
            _value1ProtoBytes = Google.Protobuf.ByteString.CopyFrom(_value1Bytes);
        }
        
        [Benchmark(Baseline = true)]
        public KeyValue MkKvListString() {
            var kv = Ahghee.Utils.PropString(_key1, _value1);
            return kv;
        }
        
        [Benchmark]
        public KeyValue MkKvArrayString()
        {
            var kv = Ahghee.Utils.PropString(_key1, _value1);
            return kv;
        }
        
        [Benchmark]
        public KeyValue MkKvManual1()
        {
            var kv = new KeyValue();
            kv.Key = Ahghee.Utils.TMDAuto(Ahghee.Utils.DBBString(_key1));
            kv.Value = Ahghee.Utils.TMDAuto(Ahghee.Utils.DBBString(_value1));
            return kv;
        }
        
        [Benchmark]
        public KeyValue MkKvManual2()
        {
            var kv = new KeyValue();
            kv.Key = Ahghee.Utils.TMDAuto(Ahghee.Utils.MetaBytes(Ahghee.Utils.metaPlainTextUtf8,_key1Bytes));
            kv.Value = Ahghee.Utils.TMDAuto(Ahghee.Utils.MetaBytes(Ahghee.Utils.metaPlainTextUtf8,_value1Bytes));
            return kv;
        }
        
        [Benchmark]
        public KeyValue MkKvManual3()
        {
            var kv = new KeyValue();
            kv.Key = Ahghee.Utils.TMDAuto(Ahghee.Utils.MetaBytesNoCopy(Ahghee.Utils.metaPlainTextUtf8,_key1ProtoBytes));
            kv.Value = Ahghee.Utils.TMDAuto(Ahghee.Utils.MetaBytesNoCopy(Ahghee.Utils.metaPlainTextUtf8,_value1ProtoBytes));
            return kv;
        }
    }

    [MinColumn, MaxColumn]
    public class CreatingTypes
    {
        [Benchmark]
        public DataBlock MkBinaryBlockString() => Ahghee.Utils.DBBString(_key1);

        [Benchmark]
        public DataBlock MkDataBlockEmpty() => new DataBlock();

        [Benchmark]
        public KeyValue MkKeyValueEmpty() => new KeyValue();

        [Benchmark]
        public Node MkNodeEmpty() => Ahghee.Utils.Node(new NodeID(), Array.Empty<KeyValue>());

        
        private static string _graph = "graph1";
        private static string _id = "12345";
        private static string _key1 = "name";
        private static string _value1 = "Austin";
        
        [Benchmark]
        public NodeID MkIdSimple() {
            var id = Ahghee.Utils.Id(_graph, _id, Ahghee.Utils.NullMemoryPointer());
            return id;
        }
        
        [Benchmark]
        public KeyValue MkKvSimple() {
            var kv = Ahghee.Utils.PropString(_key1, _value1);
            return kv;
        }
        
        [Benchmark]
        public Node MkNodeSimple() {
            var id = Ahghee.Utils.Id(_graph, _id, Ahghee.Utils.NullMemoryPointer());
            var kv = Ahghee.Utils.PropString(_key1, _value1);
            return Ahghee.Utils.Node(id, new List<KeyValue>{kv});
        }
    }

    [MinColumn, MaxColumn, BaselineColumn, AllStatisticsColumn]
    public class NodeIdHashBench
    {
        NodeID nid;
        public NodeIdHashBench()
        {
            nid = new NodeID()
            {
                Iri = "jklfdajklfjkla/ajksdfjalksjkfldas",
                Remote = "jklfsdjkflew"
            };
        }


        [Benchmark(Baseline = true)]
        public int StringHash()
        {
            return nid.Remote.GetHashCode() ^ nid.Iri.GetHashCode();
        }
        
        [Benchmark()]
        public int CustomHash()
        {
            return nid.GetHashCode();
        }
      
        [Benchmark()]
        public int MurmurHash()
        {
            return nid.GetHashCodeGoodDistribution(nid);
        }
        
    }

    [EventPipeProfiler(EventPipeProfile.CpuSampling)]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class WriteNodesBenchmark
    {
        private GrpcFileStore g;
        private IStorage istor;
        private IDisposable idis;
        private IList<Node[]> seedData;
        private Random rnd;
        public WriteNodesBenchmark()
        {
            var config = Ahghee.Program.testConfig();
            g = new GrpcFileStore(config);
            istor = g;
            idis = g;
            seedData = Ahghee.Program.buildLotsNodes(2).Take(100).ToList();
            rnd = new Random(1337);
        }
        
        [Benchmark]
        public async Task AddNodes1()
        {
            var nodes = seedData[rnd.Next(seedData.Count-1)];
            await istor.Add(nodes.Take(1));
            istor.Flush();
        }
        [Benchmark]
        public async Task AddNodes10()
        {
            var nodes = seedData[rnd.Next(seedData.Count-1)];
            await istor.Add(nodes.Take(10));
            istor.Flush();
        }
        [Benchmark]
        public async Task AddNodes100()
        {
            var nodes = seedData[rnd.Next(seedData.Count-1)];
            await istor.Add(nodes.Take(100));
            istor.Flush();
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
            var summary4 = BenchmarkRunner.Run<WriteNodesBenchmark>();
            //var summary0 = BenchmarkRunner.Run<CreatingTypes>();
            //var summary1 = BenchmarkRunner.Run<CreatingKeyValue>();
            //var summary2 = BenchmarkRunner.Run<CreatingNodeEmpty>();
            //var summary3 = BenchmarkRunner.Run<RocksDbSinglePut>();
        }
    }
}