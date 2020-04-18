using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ahghee.Grpc;
using cli.antlr;

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
        public static StringBuilder NodeIdPrinter(this StringBuilder sb, NodeID nid, int tabs)
        {
            sb.AppendLine();
            if (tabs > 0)
            {
                sb.Append(String.Empty.PadLeft(tabs, '\t'));
            }
            sb.Append("id: ");
            sb.Append(nid.Iri);
            sb.AppendLine();
                                        
            if (!string.IsNullOrEmpty(nid.Remote))
            {
                if (tabs > 0)
                {
                    sb.Append(String.Empty.PadLeft(tabs, '\t'));
                }
                sb.Append("\n  graph: ");
                sb.Append(nid.Remote);
                sb.AppendLine();
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
            sb.Append(tb.Bytes.ToBase64());
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
        
        public static StringBuilder DataPrinter(this StringBuilder sb, DataBlock db, int tabs)
        {
            return db.DataCase switch
            {
                DataBlock.DataOneofCase.B => sb.Append(db.B),
                DataBlock.DataOneofCase.D => sb.Append(db.D),
                DataBlock.DataOneofCase.F => sb.Append(db.F),
                DataBlock.DataOneofCase.I32 => sb.Append(db.I32),
                DataBlock.DataOneofCase.I64 => sb.Append(db.I64),
                DataBlock.DataOneofCase.Ui32 => sb.Append(db.Ui32),
                DataBlock.DataOneofCase.Ui64 => sb.Append(db.Ui64),
                DataBlock.DataOneofCase.Str => sb.Append(db.Str),
                DataBlock.DataOneofCase.Nodeid => NodeIdPrinter(sb, db.Nodeid, tabs+1),
                DataBlock.DataOneofCase.Metabytes => TypeBytesPrinter(sb, db.Metabytes, tabs+1),
                DataBlock.DataOneofCase.Memorypointer => MemoryPointerPrinter(sb,db.Memorypointer,tabs+1)
            };
        }

        public static void NodePrinter(this StringBuilder sb, Node n, int tabs, PrintMode pm)
        {
            sb = NodeIdPrinter(sb, n.Id, tabs);

            IEnumerable<KeyValue> kvs = null;
            if (pm != PrintMode.History)
            {
                kvs = n.Attributes
                    .GroupBy(_ => _.Key.Data,
                        (k, v) => v.OrderByDescending(_ => _.Value.Timestamp).First());
            } else
            {
                kvs = n.Attributes.OrderBy(_ => _.Value.Timestamp);
            }
            
            foreach (var attr in kvs)
            {
                if ((pm & (PrintMode.Times | PrintMode.History)) != 0)
                {
                    sb.Append("\t");
                    sb.Append(attr.Value.Timestamp);    
                }
                sb.Append("\t");
                sb = DataPrinter(sb, attr.Key.Data, tabs+2);
                sb.Append("\t: ");
                sb = DataPrinter(sb, attr.Value.Data, tabs+2);
                sb.AppendLine();
            }
            Console.Write(sb.ToString());
        }
    }
}