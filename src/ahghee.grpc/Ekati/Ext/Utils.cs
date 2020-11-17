using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ekati.Core;
using FASTER.core;
using FlatBuffers;
using Array = Ekati.Core.Array;

namespace Ekati.Ext
{
    public static class CopyToBuf
    {

        public static IEnumerable<KeyValue> AsEnumerable(this Map m)
        {
            for (int i = 0; i < m.ItemsLength; i++)
            {
                yield return m.Items(i).Value;
            }
        }

        public static Offset<Map> ToMap(this IEnumerable<KeyValue> kvs, FlatBufferBuilder buf)
        {
            var offsets =
                kvs.Select(kv => kv.CopyTo(buf))
                    .ToArray();
            return Map.CreateMap(buf, Map.CreateItemsVector(buf, offsets));
        }
        
        public static Offset<TypeBytes> CopyTo(this TypeBytes n, FlatBufferBuilder buf)
        {
            buf.StartVector(1, n.ByteArrayLength, 1);
            buf.Add(n.GetByteArrayArray());
            var vec = buf.EndVector();
            
            return TypeBytes.CreateTypeBytes(buf, 
                buf.CreateString(n.Typeiri),
                vec,
                n.Pointer.HasValue 
                    ? n.Pointer.Value.CopyTo(buf)
                    : default);
        }
        
        public static Offset<MemoryPointer> CopyTo(this MemoryPointer n, FlatBufferBuilder buf)
        {
            return MemoryPointer.CreateMemoryPointer(buf,
                n.Partitionkey,
                n.Filename,
                n.Offset,
                n.Length);
        }

        public static StringOffset CopyTo(this String n, FlatBufferBuilder buf)
        {
            return buf.CreateString(n);
        }
        
        public static Offset<NodeID> CopyTo(this NodeID n, FlatBufferBuilder buf)
        {
            return NodeID.CreateNodeID(buf,
                n.Remote.CopyTo(buf),
                n.Iri.CopyTo(buf),
                n.Pointer?.CopyTo(buf) ?? default
            );
        }

        public static Offset<Primitive> CopyTo(this Primitive n, FlatBufferBuilder buf)
        {
            if (n._type == PrimitiveType.str)
            {
                var strOff = n.Str.CopyTo(buf);
                buf.StartTable(2);
                Primitive.Add_type(buf, n._type);
                Primitive.AddStr(buf,strOff);
                return Primitive.EndPrimitive(buf);
            }
            buf.StartTable(2);
            Primitive.Add_type(buf, n._type);
            switch (n._type)
            {
                case PrimitiveType.i8: Primitive.AddI8(buf, n.I8);
                    break;
                case PrimitiveType.i16:Primitive.AddI16(buf, n.I16);
                    break;
                case PrimitiveType.i32:Primitive.AddI32(buf, n.I32);
                    break;
                case PrimitiveType.i64:Primitive.AddI64(buf, n.I64);
                    break;
                case PrimitiveType.ui8: Primitive.AddUi8(buf, n.Ui8);
                    break;
                case PrimitiveType.ui16:Primitive.AddUi16(buf, n.Ui16);
                    break;
                case PrimitiveType.ui32:Primitive.AddUi32(buf, n.Ui32);
                    break;
                case PrimitiveType.ui64:Primitive.AddUi64(buf, n.Ui64);
                    break;
                case PrimitiveType.f: Primitive.AddF(buf, n.F);
                    break;
                case PrimitiveType.f32: Primitive.AddF32(buf, n.F32);
                    break;
                case PrimitiveType.f64:
                    Primitive.AddF64(buf, n.F64);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return Primitive.EndPrimitive(buf);
        }

        public static Offset<Data> CopyTo(this Data d, FlatBufferBuilder buf)
        {
            return d.ItemType switch
            {
                DataBlock.NONE => Data.CreateData(buf, d.ItemType),
                DataBlock.Array => Data.CreateData(buf, d.ItemType, d.Item<Array>().Value.CopyTo(buf).Value),
                DataBlock.Map => Data.CreateData(buf, d.ItemType, d.Item<Map>().Value.CopyTo(buf).Value),
                DataBlock.Primitive => Data.CreateData(buf, d.ItemType, d.Item<Primitive>().Value.CopyTo(buf).Value),
                DataBlock.TypeBytes => Data.CreateData(buf, d.ItemType, d.Item<TypeBytes>().Value.CopyTo(buf).Value),
                DataBlock.NodeID => Data.CreateData(buf, d.ItemType, d.Item<NodeID>().Value.CopyTo(buf).Value),
                _ => throw new NotSupportedException($"That switch case was not found: {d.ItemType}")
            };
        }
        
        public static Offset<Array> CopyTo(this Array n, FlatBufferBuilder buf)
        {
            int len = n.ItemsLength;
            Offset<Data>[] dataOffsets = new Offset<Data>[len];
            for (int i = 0; i < len; i++)
            {
                var v = n.Items(i).Value;
                dataOffsets[i] = v.CopyTo(buf);
            }

            return Array.CreateArray(buf,
                Array.CreateItemsVector(buf, dataOffsets)
            );
        }

        public static Offset<KeyValue> CopyTo(this KeyValue n, FlatBufferBuilder buf)
        {
            return KeyValue.CreateKeyValue(buf, n.Timestamp, n.Key.Value.CopyTo(buf), n.Value.Value.CopyTo(buf));
        }

        public static Offset<Map> CopyTo(this Map n, FlatBufferBuilder buf)
        {
            int len = n.ItemsLength;
            Offset<KeyValue>[] dataOffsets = new Offset<KeyValue>[len];
            for (int i = 0; i < len; i++)
            {
                dataOffsets[i] = n.Items(i).Value.CopyTo(buf);
            }

            return Map.CreateMap(buf,
                Map.CreateItemsVector(buf, dataOffsets)
            );
        }
        public static Offset<TMD> CopyTo(this TMD n, FlatBufferBuilder buf)
        {
            return TMD.CreateTMD(buf,
                n.MetaData.HasValue ? n.MetaData.Value.CopyTo(buf) : default,
                n.Data.HasValue ? n.Data.Value.CopyTo(buf) : default);

        }

        public static Offset<Node> MergeNodeAttributesUsingBuilder(this Node input, Node output, FlatBufferBuilder builder)
        {
            var len1 = output.Attributes?.ItemsLength ?? 0;
            var len2 = input.Attributes?.ItemsLength ?? 0;
            
                var lenNew = len1 + len2;
                Offset<KeyValue>[] items = new Offset<KeyValue>[lenNew];
                for (int i = 0; i < len1; i++)
                {
                    items[i] = output.Attributes.Value.Items(i).Value.CopyTo(builder);
                }
                for (int i = 0; i < len2; i++)
                {
                    // todo: distinct?
                    items[i + len1] = input.Attributes.Value.Items(i).Value.CopyTo(builder);
                }
                var newNode = Node.CreateNode(builder, input.Id.Value.CopyTo(builder),
                    Map.CreateMap(builder, Map.CreateItemsVector(builder, items)));
                return newNode;
        }
        public static Node MergeNodeAttributesNew(this Node input, Node output)
        {
            var len1 = output.ByteBuffer == null ? 0 : output.Attributes?.ItemsLength ?? 0;
            var len2 = input.Attributes?.ItemsLength ?? 0;

            if (len1 == 0)
            {
                return input;
            }
            
            var lenNew = len1 + len2;
            Offset<KeyValue>[] items = new Offset<KeyValue>[lenNew];
            var builder = new FlatBufferBuilder(lenNew*2);
            for (int i = 0; i < len1; i++)
            {
                items[i] = output.Attributes.Value.Items(i).Value.CopyTo(builder);
            }
            for (int i = 0; i < len2; i++)
            {
                // todo: distinct?
                items[i + len1] = input.Attributes.Value.Items(i).Value.CopyTo(builder);
            }
            var newNode = Node.CreateNode(builder, input.Id.Value.CopyTo(builder),
                Map.CreateMap(builder, Map.CreateItemsVector(builder, items)));
            Node.FinishNodeBuffer(builder, newNode);
            return Node.GetRootAsNode(builder.DataBuffer);
        }
    }
    
    public class MapSerializer : BinaryObjectSerializer<Map>
    {
        public override void Deserialize(ref Map obj)
        {
            var len = this.reader.ReadInt32();
            var buf = new ByteBuffer(this.reader.ReadBytes(len));
            obj = Map.GetRootAsMap(buf);
        }

        public override void Serialize(ref Map obj)
        {
            writer.Write(obj.ByteBuffer.ToSizedArray());
        }
    }

    public class NodeIDSerializer : BinaryObjectSerializer<NodeID>
    {
        public override void Deserialize(ref NodeID obj)
        {
            var len = this.reader.ReadInt32();
            var buf = new ByteBuffer(this.reader.ReadBytes(len));
            obj = NodeID.GetRootAsNodeID(buf);
        }

        public override void Serialize(ref NodeID obj)
        {
            writer.Write(obj.ByteBuffer.ToSizedArray());
        }
    }
    
    public class NodeSerializer : BinaryObjectSerializer<Node>
    {
        public override void Deserialize(ref Node obj)
        {
            var len = this.reader.ReadInt32();
            var buf = new ByteBuffer(this.reader.ReadBytes(len));
            obj = Node.GetRootAsNode(buf);
        }

        public override void Serialize(ref Node obj)
        {
            writer.Write(obj.ByteBuffer.ToSizedArray());
        }
    }

}