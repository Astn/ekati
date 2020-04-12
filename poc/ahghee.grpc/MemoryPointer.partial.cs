
using System;
using System.Collections;
using System.Data.HashFunction.MurmurHash;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;

namespace Ahghee.Grpc
{
    public sealed partial class MemoryPointer : pb::IMessage<MemoryPointer>
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
    }

    public sealed partial class NodeID : IComparable, IComparable<NodeID>, scg.IEqualityComparer<NodeID>
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
        public override int GetHashCode() {
            return GetHashCode(this);
        }

        public bool Equals(NodeID x, NodeID y)
        {
            return x != null && x.Equals(y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(NodeID obj)
        {
            var array = new byte[System.Text.Encoding.UTF8.GetByteCount(obj.remote_) + System.Text.Encoding.UTF8.GetByteCount(obj.iri_)];
            var written = System.Text.Encoding.UTF8.GetBytes(obj.remote_,0,obj.remote_.Length,array,0);
            System.Text.Encoding.UTF8.GetBytes(obj.iri_,0,obj.iri_.Length,array,written);
            var hash = hasher.ComputeHash(array);
            return BitConverter.ToInt32(hash.Hash, 0);
        }
    }
}