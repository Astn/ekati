using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Ahghee.Grpc;
using Microsoft.Extensions.Logging;
using Antlr4;
using Antlr4.Runtime;
using BlazorFabric;
using cli.antlr;
using cli_grammer;
using Grpc.Core.Utils;

namespace UI.Pages
{
    public static class GraphHelpers
    {
        public static string ToDisplayString(this DataBlock db)
        {
            switch (db.DataCase)
            {
                case DataBlock.DataOneofCase.None: return "null";
                case DataBlock.DataOneofCase.Nodeid: return "-> " + db.Nodeid.Iri;
                case DataBlock.DataOneofCase.Metabytes:return db.Metabytes.Typeiri;
                case DataBlock.DataOneofCase.Str: return db.Str;
                case DataBlock.DataOneofCase.I32: return db.I32.ToString();
                case DataBlock.DataOneofCase.I64: return db.I64.ToString();
                case DataBlock.DataOneofCase.Ui32: return db.Ui32.ToString();
                case DataBlock.DataOneofCase.Ui64:return db.Ui64.ToString();
                case DataBlock.DataOneofCase.D: return db.D.ToString(CultureInfo.CurrentCulture);
                case DataBlock.DataOneofCase.F: return db.F.ToString(CultureInfo.CurrentCulture);
                case DataBlock.DataOneofCase.B: return db.B.ToString();
                case DataBlock.DataOneofCase.Memorypointer: return "(Void*)&pointer";
                case DataBlock.DataOneofCase.Array:
                    return $"[ {String.Join(",\n", db.Array.Item.Select(i => i.ToDisplayString()))} ]";
                case DataBlock.DataOneofCase.Map:
                    return $"{{ {String.Join(",\n", db.Map.Attributes.Select(kv =>  $"{kv.Key.Data.ToDisplayString()}: {kv.Value.Data.ToDisplayString()}")) } }}";
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}