
using System;
using System.Collections;
using System.Data.HashFunction.MurmurHash;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using FASTER.core;
using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;

namespace Ahghee.Grpc
{
    public sealed partial class MemoryPointer : IComparable<MemoryPointer>
    {
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
        public void WriteTo(pb::CodedOutputStream output) {
          //if (Partitionkey != 0) {
            output.WriteRawTag(13);
            output.WriteFixed32(Partitionkey);
          //}
          //if (Filename != 0) {
            output.WriteRawTag(21);
            output.WriteFixed32(Filename);
          //}
          //if (Offset != 0UL) {
            output.WriteRawTag(25);
            output.WriteFixed64(Offset);
          //}
          //if (Length != 0UL) {
            output.WriteRawTag(33);
            output.WriteFixed64(Length);
          //}
        }
    
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
        public int CalculateSize() {
          int size = 0;
          //if (Partitionkey != 0) {
            size += 1 + 4;
          //}
          //if (Filename != 0) {
            size += 1 + 4;
          //}
          //if (Offset != 0UL) {
            size += 1 + 8;
          //}
          //if (Length != 0UL) {
            size += 1 + 8;
          //}
          return size;
        }

        public int CompareTo(MemoryPointer other)
        {
            if (other == null) return 1;

            var byPK = this.partitionkey_.CompareTo(other.partitionkey_);
            if (byPK != 0)
            {
                return byPK;
            }

            var byfile = filename_.CompareTo(other.filename_);
            if (byfile != 0)
            {
                return byfile;
            }
            
            var byoffset = filename_.CompareTo(other.offset_);
            return byoffset != 0 ? byoffset : length_.CompareTo(other.length_);
        }
    }

    public partial class TMD : IComparable
    {
        public int CompareTo(object other)
        {
            if (other == null) return 1;

            if (other is TMD o)
            {
                var byTs = this.timestamp_.CompareTo(o.timestamp_);
                if (byTs != 0)
                    return byTs;
                
                if (this.data_ != null)
                {
                   var bydata = this.data_.CompareTo(o.data_);
                   if (bydata != 0)
                       return bydata; 
                }

                if (this.metaData_ != null)
                {
                    var bymata = this.metaData_.CompareTo(o.metaData_);
                    return bymata;                
                }
            }

            return 0;
            
        }
    }

    // public class MemoryPointerSerializer : BinaryObjectSerializer<MemoryPointer>
    // {
    //     public override void Deserialize(ref MemoryPointer obj)
    //     {
    //         throw new NotImplementedException();
    //     }
    //
    //     public override void Serialize(ref MemoryPointer obj)
    //     {
    //         throw new NotImplementedException();
    //     }
    // }
    public class PointersSerializer : BinaryObjectSerializer<Pointers>
    {
        public override void Deserialize(ref Pointers obj)
        {
            var inputStream = new pb.CodedInputStream(reader.BaseStream, true);
            obj.MergeFrom(inputStream);
        }

        public override void Serialize(ref Pointers obj)
        {
            var len = obj.CalculateSize();
            var mem = new byte[len];
            var outputStream = new pb.CodedOutputStream(mem);
            obj.WriteTo(outputStream);
            outputStream.Flush();
            writer.Write(mem);
        }
    }
    public class NodeIDSerializer : BinaryObjectSerializer<NodeID>
    {
        public override void Deserialize(ref NodeID obj)
        {
            var iriLen = reader.ReadInt32();
            obj.Iri = UTF8Encoding.Default.GetString(reader.ReadBytes(iriLen));
            var remoteLen = reader.ReadInt32();
            obj.Remote = UTF8Encoding.Default.GetString(reader.ReadBytes(remoteLen));
        }

        public override void Serialize(ref NodeID obj)
        {
            Span<byte> memory = stackalloc byte[obj.GetKeyBytesSize()];
            obj.WriteKeyBytes(memory);
            writer.Write(memory);
        }
    }
    public sealed partial class NodeID : IComparable, IComparable<NodeID>
        , scg.IEqualityComparer<NodeID>
        , IFasterEqualityComparer<NodeID>
    {
        private static IMurmurHash3 hasher = System.Data.HashFunction.MurmurHash.MurmurHash3Factory.Instance.Create();
        public int CompareTo(object obj)
        {
            switch (obj)
            {
                case null:
                    return 1;
                case NodeID other:
                    return CompareTo(other);
            }

            throw new ArgumentException("Object is not a NodeID");
        }

        public int CompareTo(NodeID other)
        {
            if (other == null)
                return 1;
            var gcompare = string.Compare(this.remote_, other.remote_, StringComparison.Ordinal);
            if (gcompare != 0)
                return gcompare;
            var nidcompare = string.Compare(this.iri_, other.iri_, StringComparison.Ordinal);
            return nidcompare;
        }
        
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
        public bool Equals(NodeID other) {
            if (ReferenceEquals(other, null)) {
                return false;
            }
            if (ReferenceEquals(other, this)) {
                return true;
            }
            if (Remote != other.Remote) return false;
            if (Iri != other.Iri) return false;
            // we don't compare pointers
            return Equals(_unknownFields, other._unknownFields);
        }

        [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode() {
            return Convert.ToInt32(GetHashCodeGoodDistribution(this));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(NodeID nodeId) {
            return Convert.ToInt32(GetHashCodeGoodDistribution(nodeId));
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Int64 GetHashCode64(NodeID nodeId) {
            return GetHashCodeGoodDistribution(nodeId);
        }

        public bool Equals(NodeID x, NodeID y)
        {
            return x != null && x.Equals(y);
        }

        // In benchmarking this one is 1.72x slower than string.GetHashCode() ^ string2.getHashCode()
        // we can't use the string.gethashcode, as it's not consistent across processes, or environments.
        // and we are using these hash codes as a why they need to be consistent
        // Currently creates a lot of collisions, which slows us down more than then murmu3 approach (GetHashCodeGoodDistribution).
        // TODO: implement murmur3 without allocations.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCodeStackAlloc(NodeID obj)
        {
            long hash = 983;
            int len = System.Text.Encoding.UTF8.GetByteCount(obj.remote_) +
                      System.Text.Encoding.UTF8.GetByteCount(obj.iri_);
            int toadd = 4 - (len % 4);
            Span<byte> array = stackalloc  byte[len + toadd];
            var written = System.Text.Encoding.UTF8.GetBytes(obj.remote_,array);
            var arra2 = array.Slice(written);
            System.Text.Encoding.UTF8.GetBytes(obj.iri_,arra2);
            var asLong = MemoryMarshal.Cast<byte,long>(array);
            unchecked
            {
                for (int i = 0; i < asLong.Length; i++)
                {
                    hash ^= asLong[i];
                }
                return hash;
            }
        }

        public int GetHashCodeIntStackAlloc(NodeID obj)
        {
            unchecked
            {
                var hash = GetHashCodeStackAlloc(obj);
                int hash32 = ((int)hash) ^ ((int)(hash >> 32));
                return hash32;
            }
        }
        // In benchmarking this one is 6.89x slower than string.GetHashCode() ^ string2.getHashCode()
        // But because it results in less collisions it's currently a net faster then our stack alloc approach.
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Int64 GetHashCodeGoodDistribution(NodeID obj)
        {
            var array = new byte[System.Text.Encoding.UTF8.GetByteCount(obj.remote_) + System.Text.Encoding.UTF8.GetByteCount(obj.iri_)];
            var written = System.Text.Encoding.UTF8.GetBytes(obj.remote_,0,obj.remote_.Length,array,0);
            System.Text.Encoding.UTF8.GetBytes(obj.iri_,0,obj.iri_.Length,array,written);
            var hash = hasher.ComputeHash(array);
            var bits = hash.BitLength;
            return bits > 32 ? BitConverter.ToInt64(hash.Hash) : BitConverter.ToInt32(hash.Hash);
        }

        public int GetKeyBytesSize()
        {
            var lens = sizeof(int) * 2;
            return lens + System.Text.Encoding.UTF8.GetByteCount(this.remote_) +
                   System.Text.Encoding.UTF8.GetByteCount(this.iri_);
        }

        public void WriteKeyBytes(Span<byte> output)
        {
            var iriLen = System.Text.Encoding.UTF8.GetByteCount(this.iri_);
            var remoteLen = System.Text.Encoding.UTF8.GetByteCount(this.remote_);
            
            // write and move
            iriLen.IntoSpan(output);
            var iriDest = output.Slice(sizeof(int));
            
            // write and move
            var offset = System.Text.Encoding.UTF8.GetBytes(this.iri_,iriDest);
            var nextPart = iriDest.Slice(offset);
            
            // write and move
            remoteLen.IntoSpan(nextPart);
            var remoteMem = nextPart.Slice(sizeof(int));
            
            // write, no move
            System.Text.Encoding.UTF8.GetBytes(this.remote_, remoteMem);
        }
        
        public bool Equals(ref NodeID x, ref NodeID y)
        {
            return x != null && x.Equals(y);
        }
        
        public long GetHashCode64(ref NodeID obj)
        {
            var array = new byte[System.Text.Encoding.UTF8.GetByteCount(obj.remote_) + System.Text.Encoding.UTF8.GetByteCount(obj.iri_)];
            var written = System.Text.Encoding.UTF8.GetBytes(obj.remote_,0,obj.remote_.Length,array,0);
            System.Text.Encoding.UTF8.GetBytes(obj.iri_,0,obj.iri_.Length,array,written);
            var hash = hasher.ComputeHash(array);
            var bits = hash.BitLength;
            return bits > 32 ? BitConverter.ToInt64(hash.Hash) : BitConverter.ToInt32(hash.Hash);
        }
    }
    
    

    public sealed partial class DataBlock : IComparable<DataBlock>, IComparable
    {
        public int CompareTo(object obj)
        {
            if (obj == null) return 1;
            if(obj is DataBlock db)
            {
                return CompareTo(db);
            }

            return 1;
        }

        public int CompareTo(DataBlock other)
        {
            if (other == null) return 1;

            if (other.DataCase != this.DataCase)
            {
                return this.DataCase - other.DataCase;
            }

            return DataCase switch
            {
                DataOneofCase.Nodeid => Nodeid.CompareTo(other.Nodeid),
                DataOneofCase.Metabytes => Metabytes.CompareTo(other.Metabytes),
                DataOneofCase.Memorypointer => Memorypointer.CompareTo(other.Memorypointer),
                DataOneofCase.B => B.CompareTo(other.B),
                DataOneofCase.D => D.CompareTo(other.D),
                DataOneofCase.F => F.CompareTo(other.F),
                DataOneofCase.I32 => I32.CompareTo(other.I32),
                DataOneofCase.I64 => I64.CompareTo(other.I64),
                DataOneofCase.Ui32 => Ui32.CompareTo(other.Ui32),
                DataOneofCase.Ui64 => Ui64.CompareTo(other.Ui64),
                DataOneofCase.Str => Str.CompareTo(other.Str),
                DataOneofCase.None => 0,
                _ => 1
            };
        }
    }

    public sealed partial class TypeBytes : IComparable<TypeBytes>
    {
        public int CompareTo(TypeBytes other)
        {
            if (other == null) return 1;

            var byType = String.Compare(Typeiri, other.Typeiri, StringComparison.Ordinal); 
            return byType != 0 ? byType : Bytes.Span.SequenceCompareTo(other.Bytes.Span);
        }
    }
}