using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using Ekati.Core;
using Ekati.Ext;
using FlatBuffers;

namespace Ekati
{
    public class NodeStore : IDisposable
    {
        private IDevice _logfile;
        private FasterKV<NodeID, Node, Node, Node, Empty, NodeStoreFuncs> _kv;
        private ClientSession<NodeID, Node, Node, Node, Empty, NodeStoreFuncs> _session;
        private IDevice _objfile;
        private Node __default = new Node();
        private bool _runCheckpointThread = true;
        private readonly string _checkpointDir;
        private volatile int serial = 0;
        public NodeStore(string file)
        {
            var continueSession = (File.Exists(file + ".ns.log")
                                   || File.Exists(file + ".ns.obj.log"));
            if (!continueSession)
            {
                File.Create(file + ".ns.log");
                File.Create(file + ".ns.obj.log");
            }
            
            _logfile = Devices.CreateLogDevice(file + ".ns.log");
            _objfile = Devices.CreateLogDevice(file + ".ns.obj.log");
            _checkpointDir = Path.Combine(Directory.GetParent(file).FullName, "ns-checkpoints");
            _kv = new FasterKV<NodeID, Node, Node, Node, Empty, NodeStoreFuncs>
                (1L << 20, new NodeStoreFuncs(), new LogSettings
            {
                LogDevice = _logfile,
                ObjectLogDevice = _objfile ,
            },
                new CheckpointSettings
                {
                    CheckpointDir = _checkpointDir,
                    CheckPointType = CheckpointType.Snapshot
                },
                new SerializerSettings<NodeID, Node>
                {
                    keySerializer = () => new NodeIDSerializer(),
                    valueSerializer = () => new NodeSerializer()
                },
                new NodeIdComparer());


            if (continueSession)
            {
                _kv.Recover();
                _session = _kv.ResumeSession("s1", out CommitPoint cp);
            }
            else
            {
                _session = _kv.NewSession("s1");
            }

            IssuePeriodicCheckpoints();
        }

        private void IssuePeriodicCheckpoints()
        {
            var t = new Thread(() => 
            {
                try
                {
                    while(_runCheckpointThread) 
                    {
                        Thread.Sleep(5 * 60000);
                        Commit();
                       
                        
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            });
            t.Start();
        }
        public long AddOrUpdateBatch(Node node)
        {
            var id = node.Id.Value;
            var n = node;
            _session.RMW(ref id, ref n, Empty.Default, serial++);
            return 1;
        }
        public bool TryPutValue(NodeID id, ref Node ptrs)
        {
       
            var status = _session.Upsert(ref id, ref ptrs, Empty.Default, serial++);
            return status == Status.OK;
        }
        public bool TryGetValue(NodeID id, ref Node ptrs)
        {
            var status = _session.Read(ref id, ref __default, ref ptrs, Empty.Default, serial++);
            return status == Status.OK;
        }

        public ValueTask<FasterKV<NodeID, Node, Node, Node, Empty, NodeStoreFuncs>.ReadAsyncResult> TryGetValueAsync(NodeID id)
        {
            return _session.ReadAsync(ref id, ref __default);
        }

        private void Commit()
        {
            _kv.Log.Flush(false);
            //_kv.ReadCache.Flush(false);
            _kv.TakeFullCheckpoint(out Guid token);
            _kv.CompleteCheckpointAsync().GetAwaiter().GetResult();
            // cleanup previous checkpoints.
            // note, if we doing incremental checkpointing then this likely doesn't work..
            var checkpoints = Directory.EnumerateDirectories(Path.Combine(_checkpointDir, "cpr-checkpoints"))
                .Concat(Directory.EnumerateDirectories(Path.Combine(_checkpointDir, "index-checkpoints")))
                .Select(di => new DirectoryInfo(di))
                .OrderBy(di => di.CreationTimeUtc)
                .Skip(4)
                .Where(dirName => dirName.Name != token.ToString());

            foreach (var oldCheckpoint in checkpoints)
            {
                // TODO: setup a checkpoint policy that lets us do full and incremenal checkpoints
                oldCheckpoint.Delete(true);
            }
        }
        public IEnumerable<Node> Iter()
        {
            var scanner = _kv.Log.Scan(_kv.Log.BeginAddress, _kv.Log.TailAddress);
            while (scanner.GetNext(out var info))
            {
                var value = scanner.GetValue();
                yield return value;
            }
        }
        public void RMW(ref NodeID key, ref Node value)
        {
            _session.RMW(ref key, ref value, Empty.Default, serial++);
        }

        public void Read(ref NodeID key, ref Node input, ref Node output)
        {
            _session.Read(ref key, ref input, ref output, Empty.Default, serial++);
        }

        public void Dispose()
        {
            _runCheckpointThread = false;
            _session?.Dispose();
            _kv?.Dispose();
            _logfile.Close();
            _objfile.Close();
        }
    }

    public class NodeIdComparer : IFasterEqualityComparer<NodeID>
    {
        public long GetHashCode64(ref NodeID k)
        {
            if (k.Remote == null)
            {
                return  ((long)k.Iri.GetHashCode() << 32);
            }
            return ((long)k.Iri.GetHashCode() << 32) | k.Remote.GetHashCode();
        }

        public bool Equals(ref NodeID k1, ref NodeID k2)
        {
            return k1.Iri.Equals(k2.Iri) && (k1.Remote == null || k2.Remote == null || k1.Remote.Equals(k2.Remote));
        }
    }

    public class NodeStoreFuncs : IFunctions<NodeID, Node, Node, Node, Empty>
    {
        public void InitialUpdater(ref NodeID key, ref Node input, ref Node value)
        {
            value = input;
        }

        public void CopyUpdater(ref NodeID key, ref Node input, ref Node oldValue, ref Node newValue)
        {
            newValue = oldValue.MergeNodeAttributesNew(input);
        }

        public bool InPlaceUpdater(ref NodeID key, ref Node input, ref Node value)
        {
            value = value.MergeNodeAttributesNew(input); //.MergeNodeAttributesNew(output);
           
            return true;
        }

        
        public void SingleReader(ref NodeID key, ref Node input, ref Node value, ref Node dst) { dst = value; }
        public void SingleWriter(ref NodeID key, ref Node src, ref Node dst) { dst = src; }
        
        public void ConcurrentReader(ref NodeID key, ref Node input, ref Node value, ref Node dst)
        {
            dst = value;//.MergeNodeAttributesNew(dst);
        }

        public bool ConcurrentWriter(ref NodeID key, ref Node src, ref Node dst)
        {
            dst = src; //.MergeNodeAttributesNew(dst); 
            return true;
        }
        
        public void ReadCompletionCallback(ref NodeID key, ref Node input, ref Node output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref NodeID key, ref Node value, Empty ctx)
        {
        }

        public void RMWCompletionCallback(ref NodeID key, ref Node input, Empty ctx, Status status)
        {
        }

        public void DeleteCompletionCallback(ref NodeID key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
        }
    }
}