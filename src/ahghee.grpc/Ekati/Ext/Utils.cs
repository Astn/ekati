using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
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

        //public static IEnumerable<KeyValue> AsEnumerable(this Map m)
        //{
        //    for (int i = 0; i < m.ItemsLength; i++)
        //    {
        //        yield return m.Items(i).Value;
        //    }
        //}

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
                case PrimitiveType.boolean: Primitive.AddBoolean(buf, n.Boolean); break;
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
            var arr = obj.ByteBuffer.ToSizedArray();
            writer.Write(arr.Length);
            writer.Write(arr);
        }
    }

    public static class MapExtensions
    {
        public static IEnumerable<Ekati.Core.KeyValue> AsEnumerable(this Map obj)
        {
            for (int i = 0; i < obj.ItemsLength; i++)
            {
                var item = obj.Items(i);
                if (item.HasValue)
                {
                    yield return item.Value;
                }
            }
        }
    }

    public static class PrimitiveExtensions
    {
        public static string AsString(this Primitive obj)
        {
            return obj._type switch
            {
                PrimitiveType.boolean => obj.Boolean.ToString(),
                PrimitiveType.f => obj.F.ToString(),
                PrimitiveType.f32 => obj.F32.ToString(),
                PrimitiveType.f64 => obj.F64.ToString(),
                PrimitiveType.i8  => obj.I8.ToString(),
                PrimitiveType.i16 => obj.I16.ToString(),
                PrimitiveType.i32 => obj.I32.ToString(),
                PrimitiveType.i64 => obj.I64.ToString(),
                PrimitiveType.ui8 => obj.Ui8.ToString(),
                PrimitiveType.ui16 => obj.Ui16.ToString(),
                PrimitiveType.ui32 => obj.Ui32.ToString(),
                PrimitiveType.ui64 => obj.Ui64.ToString(),

            };
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
            var arr = obj.ByteBuffer.ToSizedArray();
            writer.Write(arr.Length);
            writer.Write(arr);
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
            var arr = obj.ByteBuffer.ToSizedArray();
            writer.Write(arr.Length);
            writer.Write(arr);
        }
    }
    public static class DisplayHelpers
    {
        public static string ToDisplayString(this Ekati.Core.Node node)
        {
            return $"nodeId: {node.Id.ToDisplayString()}, {node.Attributes.ToDisplayString()}";
            
        }

        public static string ToDisplayString(this Ekati.Core.NodeID? nodeId)
        {
            return $"{{iri: {nodeId.Value.Iri}, remote: {nodeId.Value.Remote}}}";
        }

        public static string ToDisplayString(this Ekati.Core.Map? map)
        {
            var sb = new StringBuilder();
            sb.Append("[");
            for (int i = 0; i < map.Value.ItemsLength; i++)
            {
                sb.Append($"{{ key: {map.Value.Items(i).Value.Key.Value.Data.Value.ToDisplayString()} ,");
                sb.Append($" value: {map.Value.Items(i).Value.Value.Value.Data.Value.ToDisplayString()} }},");
            }
            sb.Append("]");
            return sb.ToString();
        }
        public static string ToDisplayString(this Ekati.Core.Data db)
        {
            switch (db.ItemType)
            {
                case DataBlock.NONE: return "null";
                case DataBlock.NodeID:
                    {
                        var nid = db.Item<NodeID>().Value;
                        return "-> " + nid.Iri;
                    }
                case DataBlock.TypeBytes:
                    {
                        var tb = db.Item<TypeBytes>().Value;
                        return tb.Typeiri;
                    }
                case DataBlock.Array:
                    {
                        var ar = db.Item<Array>().Value;
                        var itr = Enumerable.
                            Range(0, ar.ItemsLength)
                            .Select(offset => ar.Items(offset))
                            .Where(x => x.HasValue)
                            .Select(x => x.Value.ToDisplayString());
                        return $"[ {String.Join(",\n", itr)} ]";
                    }
                case DataBlock.Map:
                    {
                        var ar = db.Item<Map>().Value;
                        var itr = Enumerable.Range(0, ar.ItemsLength)
                            .Select(offset => ar.Items(offset))
                            .Where(x => x.HasValue)
                            .Select(x => x.Value)
                            .Select(kv =>
                                $"{ (kv.Key.HasValue ? kv.Key.Value.Data.Value.ToDisplayString() : "null") }: { (kv.Value.HasValue ? kv.Value.Value.Data.Value.ToDisplayString() : "null")}");
                        return $"[ {String.Join(",\n", itr)} ]";
                    }
                case DataBlock.Primitive:
                    {
                        var prim = db.Item<Primitive>().Value;
                        switch (prim._type)
                        {
                            case PrimitiveType.boolean:
                                return prim.Boolean.ToString();
                                break;
                            case PrimitiveType.str:
                                return prim.Str;
                                break;
                            case PrimitiveType.i8:
                                return prim.I8.ToString();
                                break;
                            case PrimitiveType.i16:
                                return prim.I16.ToString();
                                break;
                            case PrimitiveType.i32:
                                return prim.I32.ToString();
                                break;
                            case PrimitiveType.i64:
                                return prim.I16.ToString();
                                break;
                            case PrimitiveType.ui8:
                                return prim.Ui8.ToString();
                                break;
                            case PrimitiveType.ui16:
                                return prim.Ui16.ToString();
                                break;
                            case PrimitiveType.ui32:
                                return prim.Ui32.ToString();
                                break;
                            case PrimitiveType.ui64:
                                return prim.Ui64.ToString();
                                break;
                            case PrimitiveType.f:
                                return prim.F.ToString(CultureInfo.CurrentCulture);
                                break;
                            case PrimitiveType.f32:
                                return prim.F32.ToString(CultureInfo.CurrentCulture);
                                break;
                            case PrimitiveType.f64:
                                return prim.F64.ToString(CultureInfo.CurrentCulture);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}