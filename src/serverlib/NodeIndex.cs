using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.core;
using Google.Protobuf.Collections;

namespace Ahghee.Grpc
{
    public class NodeIndex : IDisposable
    {
        private IDevice _logfile;
        private FasterKV<NodeID, Pointers, MemoryPointer, Pointers, Empty, Funcs> _kv;
        private ClientSession<NodeID, Pointers, MemoryPointer, Pointers, Empty, Funcs> _session;
        private IDevice _objfile;
        private MemoryPointer __default = new MemoryPointer();
        private bool _runCheckpointThread = true;
        private readonly string _checkpointDir;

        public NodeIndex(string file)
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
            _kv = new FasterKV<NodeID, Pointers, MemoryPointer, Pointers, Empty, Funcs>
                (1L << 20, new Funcs(), new LogSettings
            {
                LogDevice = _logfile,
                ObjectLogDevice = _objfile 
            },
                new CheckpointSettings
                {
                    CheckpointDir = _checkpointDir,
                    CheckPointType = CheckpointType.Snapshot
                },
                new SerializerSettings<NodeID, Pointers>
                {
                    keySerializer = () => new NodeIDSerializer(),
                    valueSerializer = () => new PointersSerializer()
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
        public void AddOrUpdateBatch(IEnumerable<NodeID> ids)
        {
            foreach (var icd in ids)
            {
                var id = icd;
                var ptr = id.Pointer;
                _session.RMW(ref id, ref ptr, Empty.Default, 0);
            }
        }
        public bool TryGetValue(NodeID id, ref Pointers ptrs)
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
        public IEnumerable<Pointers> Iter()
        {
            var scanner = _kv.Log.Scan(_kv.Log.BeginAddress, _kv.Log.TailAddress);
            while (scanner.GetNext(out var info))
            {
                var value = scanner.GetValue();
                yield return value;
            }
        }

        public IEnumerable<Pointers> Iter(long fromPointer, long toPointer)
        {
            var scanner = _kv.Log.Scan(fromPointer, toPointer);
            while (scanner.GetNext(out var info))
            {
                var value = scanner.GetValue();
                yield return value;
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
        public void RMW(ref NodeID key, ref MemoryPointer value)
        {
            _session.RMW(ref key, ref value, Empty.Default, 0);
        }

        public void Read(ref NodeID key, ref MemoryPointer input, ref Pointers output)
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
    
    public class Funcs : IFunctions<NodeID, Pointers, MemoryPointer, Pointers, Empty>
    {


        public void InitialUpdater(ref NodeID key, ref MemoryPointer input, ref Pointers value)
        {
            if (value == null)
                value = new Pointers();
            if(input.Length > 0 )
                value.Pointers_.Add(input);
        }

        public void CopyUpdater(ref NodeID key, ref MemoryPointer input, ref Pointers oldValue, ref Pointers newValue)
        {
            if (newValue == null)
            {
                newValue = new Pointers();
            }
            newValue.Pointers_.AddRange(oldValue.Pointers_);
        }

        public bool InPlaceUpdater(ref NodeID key, ref MemoryPointer input, ref Pointers value) { value.Pointers_.Add(input); return true; }

        
        public void SingleReader(ref NodeID key, ref MemoryPointer input, ref Pointers value, ref Pointers dst) { dst = value; }
        public void SingleWriter(ref NodeID key, ref Pointers src, ref Pointers dst) { dst = src; }
        
        public void ConcurrentReader(ref NodeID key, ref MemoryPointer input, ref Pointers value, ref Pointers dst)
        {
            var copied = new RepeatedField<MemoryPointer>();
            if(dst!=null)
                copied.AddRange(dst.Pointers_);
            else
                dst = new Pointers();
            if(value!=null)
                copied.AddRange(value.Pointers_);
            //if(input!=null) copied.Add(input);
            
            dst.Pointers_.Clear();
            dst.Pointers_.AddRange(copied.Distinct());
        }

        public bool ConcurrentWriter(ref NodeID key, ref Pointers src, ref Pointers dst)
        {
            var copied = new RepeatedField<MemoryPointer>();
            copied.AddRange(dst.Pointers_);
            copied.AddRange(src.Pointers_);
            dst.Pointers_.Clear();
            dst.Pointers_.AddRange(copied.Distinct());
            return true;
        }
        
        public void ReadCompletionCallback(ref NodeID key, ref MemoryPointer input, ref Pointers output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref NodeID key, ref Pointers value, Empty ctx)
        {
        }

        public void RMWCompletionCallback(ref NodeID key, ref MemoryPointer input, Empty ctx, Status status)
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