using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ekati.Core;
using Ekati.Ext;
using Array = Ekati.Core.Array;

namespace cli
{
    [Flags]
    public enum PrintMode
    {
        Simple,
        History,
        Times,
        Verbose
        
    }
    
    public static class Printers
    {
        public static StringBuilder ResultsPrinter(this StringBuilder sb,
            IEnumerable<(NodeID, Either<Node, Exception>)> results)
        {
            foreach (var (nid,res) in results)
            {
                if (res.IsLeft)
                {
                    sb.NodePrinter(res.Left, 1, PrintMode.History);
                }
                else
                {
                    sb.AppendLine(res.Right.Message);
                }
            }

            return sb;
        }
        public static StringBuilder ResultsPrinter(this StringBuilder sb,
            IEnumerable<Node> results)
        {
            foreach (var node in results)
            {
                
                sb.NodePrinter(node, 1, PrintMode.History);
                
            }

            return sb;
        }
        public static StringBuilder NodeIdPrinter(this StringBuilder sb, NodeID nid, int tabs)
        {
            if (tabs > 0)
            {
                sb.Append(String.Empty.PadLeft(tabs, '\t'));
            }
            sb.Append("id: ");
            sb.Append(nid.Iri);
                                        
            if (!string.IsNullOrEmpty(nid.Remote))
            {
                if (tabs > 0)
                {
                    sb.Append(String.Empty.PadLeft(tabs, '\t'));
                }
                sb.Append("\n  graph: ");
                sb.Append(nid.Remote);
            }

            return sb;
        }
        
        public static StringBuilder TypeBytesPrinter(this StringBuilder sb,  TypeBytes tb, int tabs)
        {
            sb.AppendLine();
            if (tabs > 0)
            {
                sb.Append(String.Empty.PadLeft(tabs, '\t'));
            }
            sb.Append("type: ");
            sb.Append(tb.Typeiri);
            sb.AppendLine();

            sb.AppendLine();
            if (tabs > 0)
            {
                sb.Append(String.Empty.PadLeft(tabs, '\t'));
            }
            sb.Append("bytes: ");
            sb.Append(Convert.ToBase64String(tb.GetByteArrayArray()));
            sb.AppendLine();


            return sb;
        }
        
        public static StringBuilder MemoryPointerPrinter(this StringBuilder sb,  MemoryPointer mp, int tabs)
        {
            sb.AppendLine();
            var pad = String.Empty.PadLeft(tabs, '\t');

            sb.Append(pad);

            sb.Append("partition: ");
            sb.Append(mp.Partitionkey);
            sb.AppendLine();

            sb.AppendLine();
            sb.Append(pad);
            sb.Append("file: ");
            sb.Append(mp.Filename);
            sb.AppendLine();

            sb.AppendLine();
            sb.Append(pad);
            sb.Append("offset: ");
            sb.Append(mp.Offset);
            sb.AppendLine();
            
            sb.AppendLine();
            sb.Append(pad);
            sb.Append("length: ");
            sb.Append(mp.Length);
            sb.AppendLine();
            
            return sb;
        }
        
        public static StringBuilder DataPrinter(this StringBuilder sb, Data db, int tabs)
        {
            return db.ItemType switch
            {
                DataBlock.Primitive =>
                    db.Item<Primitive>().Value._type switch
                    {
                        PrimitiveType.f => sb.Append(db.Item<Primitive>().Value.F),
                        PrimitiveType.f32 => sb.Append(db.Item<Primitive>().Value.F32),
                        PrimitiveType.f64 => sb.Append(db.Item<Primitive>().Value.F64),
                        PrimitiveType.i8 => sb.Append(db.Item<Primitive>().Value.I8),
                        PrimitiveType.i16 => sb.Append(db.Item<Primitive>().Value.I16),
                        PrimitiveType.i32 => sb.Append(db.Item<Primitive>().Value.I32),
                        PrimitiveType.i64 => sb.Append(db.Item<Primitive>().Value.I64),
                        PrimitiveType.ui8 => sb.Append(db.Item<Primitive>().Value.Ui8),
                        PrimitiveType.ui16 => sb.Append(db.Item<Primitive>().Value.Ui16),
                        PrimitiveType.ui32 => sb.Append(db.Item<Primitive>().Value.Ui32),
                        PrimitiveType.ui64 => sb.Append(db.Item<Primitive>().Value.Ui64),
                        PrimitiveType.str => sb.Append(db.Item<Primitive>().Value.Str),
                        _ => throw new NotSupportedException($"That switch case was not found: {db.Item<Primitive>().Value._type}")
                    },
                
                DataBlock.NodeID => NodeIdPrinter(sb, db.Item<NodeID>().Value, tabs+1),
                DataBlock.TypeBytes => TypeBytesPrinter(sb, db.Item<TypeBytes>().Value, tabs+1),
                DataBlock.Array => ArrayPrinter(sb,db.Item<Array>().Value,tabs+1),
                DataBlock.Map => MapPrinter(sb, db.Item<Map>().Value,tabs+1),
                _ => throw new NotSupportedException($"That switch case was not found: {db.ItemType}")
            };
        }

        public static StringBuilder ArrayPrinter(this StringBuilder sb, Array a, int tabs)
        {
            sb.Append("[\n".PadLeft(tabs, '\t'));
            for (int i = 0; i < a.ItemsLength; i++)
            {
                if (a.Items(i).HasValue)
                    DataPrinter(sb, a.Items(i).Value, tabs + 1);
                else sb.Append("<null>,\n".PadLeft(tabs, '\t'));
            }
            sb.Append("]\n".PadLeft(tabs, '\t'));
            return sb;
        }
        public static StringBuilder MapPrinter(this StringBuilder sb, Map m, int tabs)
        {
            sb.Append("[\n".PadLeft(tabs, '\t'));
            for (int i = 0; i < m.ItemsLength; i++)
            {
                if (m.Items(i).HasValue)
                {
                    DataPrinter(sb, m.Items(i).Value.Key.Value.Data.Value, tabs + 1);
                    sb.Append(" : ");
                    DataPrinter(sb, m.Items(i).Value.Value.Value.Data.Value, tabs + 1);
                    sb.Append(" @ ");
                    sb.Append(m.Items(i).Value.Timestamp);
                }
                else
                {
                    sb.Append("<null>,\n".PadLeft(tabs, '\t'));
                }
            }
            sb.Append("]\n".PadLeft(tabs, '\t'));
            return sb;
        }
        public static StringBuilder NodePrinter(this StringBuilder sb, Node n, int tabs, PrintMode pm)
        {
            sb = NodeIdPrinter(sb, n.Id.Value, tabs);

            IEnumerable<KeyValue> kvs = null;
            if (pm != PrintMode.History)
            {
                kvs = n.Attributes.Value.AsEnumerable()
                    .GroupBy(_ => _.Key.Value.Data.Value,
                        (k, v) => v.OrderByDescending(_ => _.Timestamp).First());
            } else
            {
                kvs = n.Attributes.Value.AsEnumerable().OrderBy(_ => _.Timestamp);
            }
            
            foreach (var attr in kvs)
            {
                if ((pm & (PrintMode.Times | PrintMode.History)) != 0)
                {
                    sb.Append("\t");
                    sb.Append(attr.Timestamp);    
                }
                sb.Append("\t");
                sb = DataPrinter(sb, attr.Key.Value.Data.Value, tabs+2);
                sb.Append("\t: ");
                sb = DataPrinter(sb, attr.Value.Value.Data.Value, tabs+2);
                sb.AppendLine();
            }

            return sb;
        }
    }
}