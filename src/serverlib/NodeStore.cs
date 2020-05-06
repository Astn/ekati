using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.core;
using Google.Protobuf.Collections;

namespace Ahghee.Grpc
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
                ObjectLogDevice = _objfile 
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
                });


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
                        Thread.Sleep(60000);
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
        public void AddOrUpdateBatch(IEnumerable<Node> nodes)
        {
            foreach (var icd in nodes)
            {
                var id = icd.Id;
                var n = icd;
                _session.RMW(ref id, ref n, Empty.Default, 0);
            }
        }
        public bool TryPutValue(NodeID id, ref Node ptrs)
        {
       
            var status = _session.Upsert(ref id, ref ptrs, Empty.Default, 0);
            return status == Status.OK;
        }
        public bool TryGetValue(NodeID id, ref Node ptrs)
        {
            var status = _session.Read(ref id, ref __default, ref ptrs, Empty.Default, 0);
            return status == Status.OK;
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
            _session.RMW(ref key, ref value, Empty.Default, 0);
        }

        public void Read(ref NodeID key, ref Node input, ref Node output)
        {
            _session.Read(ref key, ref input, ref output, Empty.Default, 0);
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
    
    public class NodeStoreFuncs : IFunctions<NodeID, Node, Node, Node, Empty>
    {


        public void InitialUpdater(ref NodeID key, ref Node input, ref Node value)
        {
            value = input;
        }

        public void CopyUpdater(ref NodeID key, ref Node input, ref Node oldValue, ref Node newValue)
        {
            if (newValue == null)
            {
                newValue = new Node();
            }

            newValue.Id = oldValue.Id;
            newValue.Attributes.AddRange(oldValue.Attributes);
        }

        public bool InPlaceUpdater(ref NodeID key, ref Node input, ref Node value)
        {
            value.Id = input.Id;
            value.Attributes.AddRange(input.Attributes);
            var distinct = value.Attributes.Distinct().ToList();
            value.Attributes.Clear();
            value.Attributes.AddRange(distinct);
            return true;
        }

        
        public void SingleReader(ref NodeID key, ref Node input, ref Node value, ref Node dst) { dst = value; }
        public void SingleWriter(ref NodeID key, ref Node src, ref Node dst) { dst = src; }
        
        public void ConcurrentReader(ref NodeID key, ref Node input, ref Node value, ref Node dst)
        {
            var copied = new RepeatedField<KeyValue>();
            if(dst!=null)
                copied.AddRange(dst.Attributes);
            else
                dst = new Node();
            if(value!=null)
                copied.AddRange(value.Attributes);
            dst.Id = value.Id;
            dst.Attributes.Clear();
            dst.Attributes.AddRange(copied.Distinct());
        }

        public bool ConcurrentWriter(ref NodeID key, ref Node src, ref Node dst)
        {
            var copied = new RepeatedField<KeyValue>();
            copied.AddRange(dst.Attributes);
            copied.AddRange(src.Attributes);
            dst.Id = src.Id;
            dst.Attributes.Clear();
            dst.Attributes.AddRange(copied.Distinct());
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