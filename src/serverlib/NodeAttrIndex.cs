using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using Google.Protobuf.Collections;

namespace Ahghee.Grpc
{
    public class NodeAttrIndex : IDisposable
    {
        private IDevice _logfile;
        private FasterKV<NodeID, Attributes, Attributes, Attributes, Empty, NodeAttrIndexFuncs> _kv;
        private ClientSession<NodeID, Attributes, Attributes, Attributes, Empty, NodeAttrIndexFuncs> _session;
        private IDevice _objfile;
        private Attributes __default = new Attributes();
        private bool _runCheckpointThread = true;
        private readonly string _checkpointDir;

        public NodeAttrIndex(string file)
        {
            var continueSession = (File.Exists(file + ".log")
                                   || File.Exists(file + ".obj.log"));
            if (!continueSession)
            {
                File.Create(file + ".log");
                File.Create(file + ".obj.log");
            }
            
            _logfile = Devices.CreateLogDevice(file + ".log");
            _objfile = Devices.CreateLogDevice(file + ".obj.log");
            _checkpointDir = Path.Combine(Directory.GetParent(file).FullName, "checkpoints");
            _kv = new FasterKV<NodeID, Attributes, Attributes, Attributes, Empty, NodeAttrIndexFuncs>
                (1L << 20, new NodeAttrIndexFuncs(), new LogSettings
            {
                LogDevice = _logfile,
                ObjectLogDevice = _objfile 
            },
                new CheckpointSettings
                {
                    CheckpointDir = _checkpointDir,
                    CheckPointType = CheckpointType.Snapshot
                },
                new SerializerSettings<NodeID, Attributes>
                {
                    keySerializer = () => new NodeIDSerializer(),
                    valueSerializer = () => new AttributesSerializer()
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
            foreach (var node in nodes)
            {
                var id = node.Id;
                var ptr = new Attributes();
                ptr.Attributes_.AddRange(node.Attributes);
                _session.RMW(ref id, ref ptr, Empty.Default, 0);
            }
        }
        public bool TryGetValue(NodeID id, ref Attributes ptrs)
        {
            var status = _session.Read(ref id, ref __default, ref ptrs, Empty.Default, 0);
            return status == Status.OK;
        }
        
        public ValueTask<FasterKV<NodeID, Attributes, Attributes, Attributes, Empty, NodeAttrIndexFuncs>.ReadAsyncResult> TryGetValueAsync(NodeID id)
        {
            return _session.ReadAsync(ref id, ref __default, Empty.Default);
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
                var node = new Node();
                node.Id = scanner.GetKey();
                var attrs = scanner.GetValue();
                node.Attributes.AddRange(attrs.Attributes_);
                
                yield return node;
            }
        }

        public IEnumerable<Node> Iter(long fromPointer, long toPointer)
        {
            var scanner = _kv.Log.Scan(fromPointer, toPointer);
            while (scanner.GetNext(out var info))
            {
                var node = new Node();
                node.Id = scanner.GetKey();
                var attrs = scanner.GetValue();
                node.Attributes.AddRange(attrs.Attributes_);
                
                yield return node;
            }
        }
        public long CurrentHeadAddr()
        {
            return _kv.Log.HeadAddress;
        }
        public long CurrentTailAddr()
        {
            return _kv.Log.TailAddress;
        }
        public void RMW(ref NodeID key, ref Attributes value)
        {
            _session.RMW(ref key, ref value, Empty.Default, 0);
        }

        public void Read(ref NodeID key, ref Attributes input, ref Attributes output)
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
    
    public class NodeAttrIndexFuncs : IFunctions<NodeID, Attributes, Attributes, Attributes, Empty>
    {


        public void InitialUpdater(ref NodeID key, ref Attributes input, ref Attributes value)
        {
            if (value == null)
                value = new Attributes();
            value.Attributes_.AddRange(input.Attributes_);
        }

        public void CopyUpdater(ref NodeID key, ref Attributes input, ref Attributes oldValue, ref Attributes newValue)
        {
            if (newValue == null)
            {
                newValue = new Attributes();
            }
            newValue.Attributes_.AddRange(oldValue.Attributes_);
        }

        public bool InPlaceUpdater(ref NodeID key, ref Attributes input, ref Attributes value) { value.Attributes_.AddRange(input.Attributes_); return true; }

        
        public void SingleReader(ref NodeID key, ref Attributes input, ref Attributes value, ref Attributes dst) { dst = value; }
        public void SingleWriter(ref NodeID key, ref Attributes src, ref Attributes dst) { dst = src; }
        
        public void ConcurrentReader(ref NodeID key, ref Attributes input, ref Attributes value, ref Attributes dst)
        {
            var copied = new RepeatedField<KeyValue>();
            if(dst!=null)
                copied.AddRange(dst.Attributes_);
            else
                dst = new Attributes();
            if(value!=null)
                copied.AddRange(value.Attributes_);
            //if(input!=null) copied.Add(input);
            
            dst.Attributes_.Clear();
            dst.Attributes_.AddRange(copied.Distinct());
        }

        public bool ConcurrentWriter(ref NodeID key, ref Attributes src, ref Attributes dst)
        {
            var copied = new RepeatedField<KeyValue>();
            copied.AddRange(dst.Attributes_);
            copied.AddRange(src.Attributes_);
            dst.Attributes_.Clear();
            dst.Attributes_.AddRange(copied.Distinct());
            return true;
        }
        
        public void ReadCompletionCallback(ref NodeID key, ref Attributes input, ref Attributes output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref NodeID key, ref Attributes value, Empty ctx)
        {
        }

        public void RMWCompletionCallback(ref NodeID key, ref Attributes input, Empty ctx, Status status)
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