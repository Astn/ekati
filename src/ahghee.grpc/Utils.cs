using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ekati.Core;
using FlatBuffers;
using Array = Ekati.Core.Array;

namespace Ekati
{
    public static class Utils
    {
        public static void IntoSpan(this int number, Span<byte> dest)
        {
            var sizeBytes = BitConverter.GetBytes(number);
            for (var i = 0; i < sizeBytes.Length; i++)
            {
                dest[i] = sizeBytes[i];
            }
        }

        public static int GetPartitionFromHash(int partitionCount, NodeID nid)
        {
            return nid.Iri.GetHashCode() % partitionCount;
        }

        public static string metaPlainTextUtf8 = "xs:string";
        public static string metaXmlInt = "xs:int";
        public static string metaXmlDouble = "xs:double";


        public static Offset<TypeBytes> MetaBytesNoCopy(FlatBufferBuilder builder, string typ, byte[] bytes)
        {
            builder.StartVector(1, bytes.Length, 0);
            builder.Add(bytes);
            var bts = builder.EndVector();
            var str = builder.CreateString(typ);
            var tb = TypeBytes.CreateTypeBytes(builder, str, bts);
            return tb;
        }

        public static Offset<TypeBytes> MetaBytes (FlatBufferBuilder builder, string typ, byte[] bytes) 
        {
            return MetaBytesNoCopy( builder ,typ, bytes);
        }

        public static Offset<MemoryPointer> nmp(FlatBufferBuilder builder)
        {
            return MemoryPointer.CreateMemoryPointer(builder, 0, 0, 0, 0);
        }
    
        public static Offset<MemoryPointer> NullMemoryPointer(FlatBufferBuilder builder)
        {
            return nmp(builder);
        }
    
    
        public static Offset<NodeID> Id(FlatBufferBuilder builder, string graph, string iri)
        {
            var g = builder.CreateString(graph);
            var id = builder.CreateString(iri);
            var pointer = nmp(builder);
            return NodeID.CreateNodeID(builder, g, id, pointer);
        }
    
        public static Offset<Data> PrimitiveBool(FlatBufferBuilder builder, bool value)
        {
            Primitive.StartPrimitive(builder);
            Primitive.AddBoolean(builder, value);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }
        
        public static Offset<Data> NumInt(FlatBufferBuilder builder, int value)
        {
            Primitive.StartPrimitive(builder);
            Primitive.AddI32(builder, value);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }
        
        public static Offset<Data> NumLong(FlatBufferBuilder builder, long value)
        {
            Primitive.StartPrimitive(builder);
            Primitive.AddI64(builder, value);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }
        
        public static Offset<Data> NumULong(FlatBufferBuilder builder, UInt64 value)
        {
            Primitive.StartPrimitive(builder);
            Primitive.AddUi64(builder, value);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }

        public static Offset<Data> NumFloat(FlatBufferBuilder builder, float value)
        {
            Primitive.StartPrimitive(builder);
            Primitive.AddF32(builder, value);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }

        public static Offset<Data> DataId(FlatBufferBuilder builder, Offset<NodeID> id)
        {
            return Data.CreateData(builder, DataBlock.NodeID, id.Value);
        }
        public static Offset<Data> DataMap(FlatBufferBuilder builder, Offset<Map> value)
        {
            return Data.CreateData(builder, DataBlock.Map, value.Value);
        }
        
        public static Offset<Data> DataArray(FlatBufferBuilder builder, Offset<Array> value)
        {
            return Data.CreateData(builder, DataBlock.Array, value.Value);
        }

        public static Offset<Data> DataTypeBytes(FlatBufferBuilder builder, Offset<TypeBytes> value)
        {
            return Data.CreateData(builder, DataBlock.TypeBytes, value.Value);
        }
        
        
        public static Offset<Data> NumDouble(FlatBufferBuilder builder, double value)
        {
            Primitive.StartPrimitive(builder);
            Primitive.AddF64(builder, value);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }

        public static Offset<Data> DBString(FlatBufferBuilder builder, string text)
        {
            var str = builder.CreateString(text);
            Primitive.StartPrimitive(builder);
            Primitive.AddStr(builder, str);
            return Data.CreateData(builder, DataBlock.Primitive, Primitive.EndPrimitive(builder).Value);
        }

        public static Offset<TMD> Tmd(FlatBufferBuilder builder, Offset<Data> data)
        {
            TMD.StartTMD(builder);
            TMD.AddData(builder, data);
            return TMD.EndTMD(builder);
        }

        public static Offset<TMD> MetaData(FlatBufferBuilder builder, Offset<Data> data, Offset<Data> metaData)
        {
            TMD.StartTMD(builder);
            TMD.AddData(builder, data);
            TMD.AddMetaData(builder, metaData);
            return TMD.EndTMD(builder);
        }

        public static Offset<KeyValue> Prop(FlatBufferBuilder builder, long time, Offset<TMD> key, Offset<TMD> value) {

            return KeyValue.CreateKeyValue(
                                   builder,
                                   time,
                                   key,
                                   value
                               );
        }

        public static Offset<KeyValue> PropString(FlatBufferBuilder builder, string key, string value, long time) {
            return Prop(builder, time, Tmd(builder, DBString(builder, key)), Tmd(builder, DBString(builder, value)));
        }
        public static Offset<KeyValue> PropInt(FlatBufferBuilder builder, string key, int value, long time)
        {
            return Prop(builder, time, Tmd(builder, DBString(builder, key)), Tmd(builder, NumInt(builder, value)));
        }
        public static Offset<KeyValue> PropDouble(FlatBufferBuilder builder, string key, double value, long time)
        {
            return Prop(builder, time, Tmd(builder, DBString(builder, key)), Tmd(builder, NumDouble(builder, value)));
        }

        public static Offset<KeyValue> PropData(FlatBufferBuilder builder, string key, Offset<Data> value, long time)
        {
            return Prop(builder, time, Tmd(builder, DBString(builder, key)), Tmd(builder, value));
        }

        public static Offset<Node> Nodee(FlatBufferBuilder builder, Offset<NodeID> nid, Offset<Ekati.Core.Map> attrs)
        {
            return Node.CreateNode(builder, nid, attrs);
        }
        
    }
   

}