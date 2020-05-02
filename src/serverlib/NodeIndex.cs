using System;
using System.Linq;
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

        public NodeIndex(string file)
        {
            _logfile = Devices.CreateLogDevice(file + ".log");
            _objfile = Devices.CreateLogDevice(file + ".obj.log");

            _kv = new FasterKV<NodeID, Pointers, MemoryPointer, Pointers, Empty, Funcs>
                (1L << 20, new Funcs(), new LogSettings
            {
                LogDevice = _logfile,
                ObjectLogDevice = _objfile
            },
                null,
                new SerializerSettings<NodeID, Pointers>
                {
                    keySerializer = () => new NodeIDSerializer(),
                    valueSerializer = () => new PointersSerializer()
                });
            _session = _kv.NewSession();
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

        public void CopyUpdater(ref NodeID key, ref MemoryPointer input, ref Pointers oldValue, ref Pointers newValue) => newValue.Pointers_.AddRange(oldValue.Pointers_);

        public bool InPlaceUpdater(ref NodeID key, ref MemoryPointer input, ref Pointers value) { value.Pointers_.Add(input); return true; }

        
        public void SingleReader(ref NodeID key, ref MemoryPointer input, ref Pointers value, ref Pointers dst) { dst = value; }
        public void SingleWriter(ref NodeID key, ref Pointers src, ref Pointers dst) { dst = src; }
        
        public void ConcurrentReader(ref NodeID key, ref MemoryPointer input, ref Pointers value, ref Pointers dst)
        {
            var copied = new RepeatedField<MemoryPointer>();
            copied.AddRange(dst.Pointers_);
            copied.AddRange(value.Pointers_);
            //if(input!=null) copied.Add(input);
            dst.Pointers_.Clear();
            dst.Pointers_.AddRange(copied);
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