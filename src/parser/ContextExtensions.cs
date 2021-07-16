using System;
using System.Collections.Generic;
using System.Diagnostics;

using System.Text;

using Ekati.Core;
using FlatBuffers;
using Array = Ekati.Core.Array;

using parser_grammer;
using Utils = Ekati.Utils;

namespace cli
{
    public static class ContextExtensions
    {
        public static Offset<NodeID> ToNodeID(this AHGHEEParser.NodeidContext id, FlatBufferBuilder builder)
        {
            var json = id.obj();
            var text = json!=null ? json.GetText() : id.GetText().Trim('"');                                
            
            
            var split = text.Split('/');
            var graph = String.Empty;
            if (split.Length > 1)
            {
                graph = split[0];
            }
            var ab = Ekati.Utils.Id(builder, graph, text);
            return ab;
            

            return Ekati.Utils.Id(builder, "", "");
        }

        public static Offset<TMD> ToDataBlock(this NTRIPLESParser.ObjContext ctx, FlatBufferBuilder builder, Func<string, string> BNToId)
        {
            // VALUE with optional stuffs
            var literalString = ctx.literal()?.STRING()?.GetText().Trim('"');
            
            if (literalString != null)
            {
                var str = Utils.DBString(builder, literalString);
                
                var literalStringTypeIri = ctx.literal()?.IRI()?.GetText().Trim('<', '>');
                var literalStringLang = ctx.literal()?.LANGTAG()?.GetText();
                if (literalStringTypeIri != null)
                {
                    return Utils.MetaData(builder, str, Utils.DataId(builder, Utils.Id(builder, "", literalStringTypeIri)));
                }

                if (literalStringLang != null)
                {
                    return Utils.MetaData(builder, str, Utils.DBString(builder, $"lang:{literalStringLang}"));

                }
                return Utils.Tmd(builder, str);
            }
            
            // Value Pointer
            var objPointer = ctx.IRI()?.GetText()?.Trim('<', '>');
            if (objPointer != null)
            {
                return Utils.Tmd(builder, Utils.DataId(builder, Utils.Id(builder, "", objPointer)));
            }
            
            // Value BlankNode, got this far, we have to have a blankNode, or something is broken.
            var objVal = ctx.BLANKNODE().GetText();
        
            var objBn = BNToId(objVal);
            return Utils.Tmd(builder, Utils.DataId(builder, Utils.Id(builder, "", objBn)));
        }
        //
        public static Offset<Data> ToDataBlockNodeID(this string str, FlatBufferBuilder builder)
        {
            return Utils.DataId(builder, Utils.Id(builder, "", str));

        }
        public static Offset<Data> ToDataBlock(this NTRIPLESParser.PredContext ctx, FlatBufferBuilder builder)
        {
            var pred = ctx.GetText().Trim('<', '>');
            return pred.ToDataBlockNodeID(builder);
        }
        //
        public static Offset<NodeID> ToNodeId(this NTRIPLESParser.SubjContext ctx, FlatBufferBuilder builder, Func<string,string> BNToId)
        {
            var subIRI = ctx.IRI()?.GetText()?.Trim('<', '>');
            if (subIRI == null)
            {
                var subjBN = ctx.BLANKNODE().GetText();
                subIRI = BNToId(subjBN);
            }

            return Utils.Id(builder, "", subIRI);
        }
        //
        // public static NodeID ToNodeID(this string str)
        // {
        //     return new NodeID{Iri = str};
        // }
        
        //
        public static Offset<Array> ToArray(this AHGHEEParser.ArrContext arr, FlatBufferBuilder builder)
        {
            var inputData = arr.value();
            var items = new Offset<Data>[inputData.Length];
            for (int i = 0; i < inputData.Length; i++)
            {
                items[i] = inputData[i].ToData(builder);
            }

            var vec = Array.CreateItemsVector(builder, items);
            return Array.CreateArray(builder, vec);
        }
        public static Offset<Map> ToMap(this AHGHEEParser.KvpsContext kvps, FlatBufferBuilder builder)
        {
            try
            {
                var items = new List<Offset<KeyValue>>();
                foreach (var pair in kvps.pair())
                {
                    Offset<KeyValue> kvp = default(Offset<KeyValue>);
                    if (pair.kvp() != null)
                    {
                        var key = pair.kvp().STRING().GetText().Trim('"'); 
                        var value = pair.kvp().value().ToData(builder);
                        kvp = Utils.PropData(builder, key, value, 0L);
                    } else if(pair.edge()!=null)
                    {
                        var key= pair.edge().STRING(0).GetText().Trim('"');
                        var value = pair.edge().STRING(1).GetText().Trim('"');
                        var id = Utils.Id(builder, "", value);
                        kvp = Utils.PropData(builder, key, Utils.DataId(builder, id), 0L);
                    }else if(pair.fedge()!=null)
                    {
                        var key= pair.edge().STRING(0).GetText().Trim('"');
                        var id =Utils.Id(builder, "", key);
                        var value = pair.edge().STRING(1).GetText().Trim('"');
                        kvp = Utils.Prop(builder, 0L, Utils.Tmd(builder, Utils.DataId(builder, id)),
                            Utils.Tmd(builder, Utils.DBString(builder, value)));
                    }else if(pair.dedge()!=null)
                    {
                        var key= pair.edge().STRING(0).GetText().Trim('"');
                        var value = pair.edge().STRING(1).GetText().Trim('"');
                        var keyId = Utils.Id(builder, "", key);
                        var valueId = Utils.Id(builder, "", value);
                        kvp = Utils.Prop(builder, 0L, Utils.Tmd(builder, Utils.DataId(builder, keyId)),
                            Utils.Tmd(builder, Utils.DataId(builder, valueId)));
                    }
                    else
                    {
                        continue;
                    }
                    items.Add(kvp);         
                }

                var vec = Map.CreateItemsVector(builder, items.ToArray());
                return Map.CreateMap(builder, vec);
            }
            catch (Exception ex)
            {
                Console.WriteLine(kvps.GetText());
                Console.WriteLine(ex.Message + "\n" + ex.StackTrace);
                throw;
            }
        
            
        }
        public static Offset<Data> ToData(this AHGHEEParser.ValueContext v, FlatBufferBuilder builder)
        {
            Offset<Data> data = default(Offset<Data>);
        
            if (v.STRING() != null)
            {
                data = Utils.DBString(builder, v.STRING().GetText().Trim('"'));
            } else if (v.NUMBER() != null)
            {
                var numberstr = v.NUMBER().GetText();
                if (Int32.TryParse(numberstr, out var i32))
                {
                    data = Utils.NumInt(builder, i32);
                } else if (Int64.TryParse(numberstr, out var i64))
                {
                    data = Utils.NumLong(builder, i64);
                } else if (UInt64.TryParse(numberstr, out var ui64))
                {
                    data = Utils.NumULong(builder, ui64);
                } else if (Single.TryParse(numberstr, out var sing))
                {
                    data = Utils.NumFloat(builder, sing);
                }
                else if(Double.TryParse(numberstr, out var doub))
                {
                    data = Utils.NumDouble(builder, doub);
                }
            } else if (v.obj() != null && v.obj().kvps() != null)
            {
                data = Utils.DataMap(builder, v.obj().kvps().ToMap(builder));
       
            } else if (v.arr() != null)
            {
                data = Utils.DataArray(builder, v.arr().ToArray(builder));
        
            } else if (!string.IsNullOrWhiteSpace(v.GetText()))
            {
                var t = v.GetText().Trim('"');
                if (Boolean.TryParse(t, out var boo))
                {
                    data = Utils.PrimitiveBool(builder, boo);
                } else if (t == "null")
                {
                    // do nothing.    
                }
            }
            // json fallback
            // todo: allow smart strings such as xml/json etc.. maybe just let them
            // specify the type before or after the string content.
            // could also let them pass binary in base64 or something
            if (data.Value == 0)
            {
                Debugger.Break();
                var jsonstuff = v.GetText(); // must be some kind of json at this point.. (I hope :P)
                var tb = Utils.MetaBytesNoCopy(builder, "application/json", Encoding.UTF8.GetBytes(jsonstuff));
                data = Utils.DataTypeBytes(builder, tb);              
            }
        
            return data;
        }
        //
        // public static FollowOperator.Types.FollowAny ToFollowAny(this AHGHEEParser.AnynumContext ctx)
        // {
        //     return new FollowOperator.Types.FollowAny()
        //     {
        //         Range = ctx.range()?.ToRange()
        //     };
        // }
        //
        // public static Range ToRange(this AHGHEEParser.RangeContext ctx)
        // {
        //     return new Range()
        //     {
        //         From = Int32.Parse(ctx.@from()?.GetText() ?? "0"),
        //         To = Int32.Parse(ctx.to().GetText() ?? "0")
        //     };
        // }
        //
        // public static FollowOperator.Types.EdgeNum ToEdgeNum(this AHGHEEParser.EdgenumContext ctx)
        // {
        //     var e = new FollowOperator.Types.EdgeNum();
        //     var db = ctx.value()?.ToDataBlock();
        //     if (db != null)
        //     {
        //         e.EdgeRange = new FollowOperator.Types.EdgeRange
        //         {
        //             Edge = db,
        //             Range = ctx.range().ToRange()
        //         };
        //     }
        //     else
        //     {
        //         e.EdgeCmp = new FollowOperator.Types.EdgeCMP()
        //         {
        //             Left = ctx.edgenum(0).ToEdgeNum(),
        //             BOOLOP =ctx.BOOLOP().GetText(),
        //             Right = ctx.edgenum(1).ToEdgeNum()
        //         };
        //     }
        //
        //     return e;
        // }
        // public static FollowOperator ToFollowOperator(this AHGHEEParser.FollowContext ctx)
        // {
        //     var fo = new FollowOperator();
        //
        //     if (ctx.anynum() != null)
        //     {
        //         fo.FollowAny = ctx.anynum().ToFollowAny();
        //     }
        //     else
        //     {
        //         fo.FollowEdge = ctx.edgenum().ToEdgeNum();
        //     }
        //     return fo;
        // }
        //
        // public static FilterOperator.Types.Compare ToCompare(this AHGHEEParser.CompareContext ctx)
        // {
        //     var key = ctx.wfkey()?.value().ToDataBlock();
        //     if (key!=null)
        //     {
        //         var value = ctx.wfvalue()?.value().ToDataBlock();
        //         return new FilterOperator.Types.Compare()
        //         {
        //             KevValueCmp = new FilterOperator.Types.CompareKeyValue()
        //             {
        //                 Property = key,
        //                 MATHOP = ctx.MATHOP().GetText(),
        //                 Value = value
        //             }
        //         };
        //     }
        //
        //     var left = ctx.compare(0);
        //     var right = ctx.compare(1);
        //     
        //     return new FilterOperator.Types.Compare()
        //     {
        //         CompoundCmp = new FilterOperator.Types.CompareCompound()
        //         {
        //             Left = left.ToCompare(),
        //             BOOLOP = ctx.BOOLOP().GetText(),
        //             Right = right.ToCompare()
        //         }
        //     };
        // }
        //
        // public static FilterOperator ToFilterOperator(this AHGHEEParser.WherefilterContext ctx)
        // {
        //     var filter = new FilterOperator();
        //     filter.Compare = ctx.compare()?.ToCompare();
        //     return filter;
        // }
        //
        // public static SkipFilter ToSkipOperator(this AHGHEEParser.SkipfilterContext ctx)
        // {
        //     var filter = new SkipFilter();
        //     filter.Value = Int32.Parse(ctx.NUMBER().GetText());
        //     return filter;
        // }
        //
        // public static LimitFilter ToLimitOperator(this AHGHEEParser.LimitfilterContext ctx)
        // {
        //     var filter = new LimitFilter();
        //     filter.Value = Int32.Parse(ctx.NUMBER().GetText());
        //     return filter;
        // }
        //
        // public static FieldsOperator.Types.Clude ToClude(this AHGHEEParser.CludeContext ctx)
        // {
        //     FieldsOperator.Types.CludeOp.Types.CludePart ToCludePart(AHGHEEParser.CludepartContext cludeOpChild)
        //     {
        //         var cp = new FieldsOperator.Types.CludeOp.Types.CludePart();
        //         if (cludeOpChild.CARET() != null && cludeOpChild.STRING() != null)
        //         {
        //             cp.CarrotStringMatch = cludeOpChild.STRING().GetText().Trim('"');
        //         }else if (cludeOpChild.STRING() != null)
        //         {
        //             cp.StringMatch = cludeOpChild.STRING().GetText().Trim('"');
        //         }else if (cludeOpChild.CARET() != null)
        //         {
        //             cp.IsCaret = true;
        //         }else if (cludeOpChild.STAR() != null)
        //         {
        //             cp.IsStar = true;
        //         }else if (cludeOpChild.TYPEINT() != null)
        //         {
        //             cp.IsTypeInt = true;
        //         }else if (cludeOpChild.TYPEFLOAT() != null)
        //         {
        //             cp.IsTypeFloat = true;
        //         }else if (cludeOpChild.TYPESTRING() != null)
        //         {
        //             cp.IsTypeString = true;
        //         }
        //         
        //         return cp;
        //     }
        //     
        //     var clude = new FieldsOperator.Types.Clude();
        //     if (ctx.cludeop() != null)
        //     {
        //         var cludeop = ctx.cludeop();
        //         clude.Op = new FieldsOperator.Types.CludeOp();
        //         if (cludeop.ChildCount > 1)
        //         {
        //             clude.Op.Left = ToCludePart(cludeop.cludepart(0));
        //             clude.Op.Right = ToCludePart(cludeop.cludepart(1));
        //         }
        //     } else if (ctx.include() != null)
        //     {
        //         if (ctx.clude() != null && ctx.clude().Length == 1)
        //         {
        //             clude.Twoclude = new FieldsOperator.Types.TwoClude();
        //             clude.Twoclude.Include = ToClude(ctx.include().clude());
        //             clude.Twoclude.Left = ToClude(ctx.clude(0));
        //         }
        //         else
        //         {
        //             clude.Include = ToClude(ctx.include().clude());
        //         }
        //     } else if (ctx.exclude() != null)
        //     {
        //         if (ctx.clude() != null && ctx.clude().Length == 1)
        //         {
        //             clude.Twoclude = new FieldsOperator.Types.TwoClude();
        //             clude.Twoclude.Exclude = ToClude(ctx.exclude().clude());
        //             clude.Twoclude.Left = ToClude(ctx.clude(0));
        //         }
        //         else
        //         {
        //             clude.Include = ToClude(ctx.include().clude());
        //         }
        //     } else if (ctx.clude() != null && ctx.clude().Length > 0)
        //     {
        //         clude.List = new FieldsOperator.Types.CludeList();
        //         foreach (var c in ctx.clude())
        //         {
        //            clude.List.Cludes.Add(ToClude(c)); 
        //         }
        //     }
        //     return clude;
        // }
        // public static FieldsOperator ToFieldsOperator(this AHGHEEParser.FieldsContext ctx)
        // {
        //     var fields = new FieldsOperator();
        //     fields.Clude = ctx.clude().ToClude();
        //     return fields;
        // }
        //
        // public static Step ToPipeFlow(this AHGHEEParser.PipeContext ctx)
        // {
        //     var cmd = ctx.pipecmd();
        //     if (cmd.follow() != null)
        //     {
        //         var follow = cmd.follow().ToFollowOperator();
        //         return new Step()
        //         {
        //             Follow = follow,
        //             Next = ctx.pipe()?.ToPipeFlow()
        //         };
        //     } 
        //     
        //     if (cmd.wherefilter() != null)
        //     {
        //         var filter = cmd.wherefilter().ToFilterOperator();
        //         return new Step()
        //         {
        //             Filter = filter,
        //             Next = ctx.pipe()?.ToPipeFlow()
        //         };
        //     }
        //
        //     if (cmd.skipfilter() != null)
        //     {
        //         var skip = cmd.skipfilter().ToSkipOperator();
        //         return new Step()
        //         {
        //             Skip = skip,
        //             Next = ctx.pipe()?.ToPipeFlow()
        //         };
        //     }
        //     
        //     if (cmd.limitfilter() != null)
        //     {
        //         var limit = cmd.limitfilter().ToLimitOperator();
        //         return new Step()
        //         {
        //             Limit = limit,
        //             Next = ctx.pipe()?.ToPipeFlow()
        //         };
        //     }
        //
        //     if (cmd.fields() != null)
        //     {
        //         var fields = cmd.fields().ToFieldsOperator();
        //         return new Step()
        //         {
        //             Fields = fields,
        //             Next = ctx.pipe()?.ToPipeFlow()
        //         };
        //     }
        //     
        //     throw new NotImplementedException("Only Follow and Filter are available in PipeContext so far.");
        // }
    }
}