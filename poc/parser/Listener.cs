using Antlr4.Runtime;
using System.IO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ahghee;
using Ahghee.Grpc;
using Antlr4.Runtime.Tree;
using cli_grammer;
using Google.Protobuf;


namespace cli.antlr
{
    public class Listener : AHGHEEBaseListener
    {
       //private readonly IStorage _store;
        private readonly Func<IEnumerable<Node>, Task> _adder;
        private bool flushed = false;
        private readonly Action<IEnumerable<NodeID>, Step> _query;
        private readonly Action _flush;

        public Listener(Func<IEnumerable<Node>,Task> adder, Action<IEnumerable<NodeID>, Step> query, Action flush)
        {
            // _store = store;
            _adder = adder;
            _query = query;
            _flush = flush;
        }

        public override void ExitPut(AHGHEEParser.PutContext context){
            var pm = GetPrintMode(context.flags());  
            foreach(var nc in context.node()){

                List<Node> _nodes = new List<Node>();
                if (nc.obj() != null)
                {
                    var n = JsonParser.Default.Parse<Node>( nc.GetText() );
                    n.Id.Pointer = Utils.NullMemoryPointer();
                    n.Fragments.Add(Utils.NullMemoryPointer());
                    n.Fragments.Add(Utils.NullMemoryPointer());
                    n.Fragments.Add(Utils.NullMemoryPointer());
                    _nodes.Add(n);
                }
                else if(nc.nodeid()!= null && nc.kvps() != null)
                {
                    var n = new Node();
                    n.Id = new NodeID();
                    if (nc.nodeid().obj() != null)
                    {
                        n.Id = JsonParser.Default.Parse<NodeID>(nc.nodeid().obj().GetText());
                    }
                    else
                    {
                        if (nc.nodeid().remote() != null)
                        {
                            n.Id.Remote = nc.nodeid().remote().GetText().Trim('"');
                        }

                        n.Id.Iri = nc.nodeid().id().GetText().Trim('"');
                    }

                    var vs = new List<KeyValue>();
                    if (nc.kvps() != null)
                    {
                        var kvps = nc.kvps();
                        if (kvps == null)
                        {
                            kvps = nc.obj()?.kvps();
                        }

                        foreach (var pair in kvps.pair())
                        {
                            var kv = new KeyValue();
                            kv.Key = new TMD();
                            kv.Key.Data = new DataBlock();
                            kv.Value = new TMD();
                            if (pair.kvp() != null)
                            {
                                kv.Value.Data = pair.kvp().value().ToDataBlock();
                                kv.Key.Data.Str = pair.kvp().STRING().GetText().Trim('"');    
                            } else if(pair.edge()!=null)
                            {
                                kv.Key.Data.Str = pair.edge().STRING(0).GetText().Trim('"');
                                kv.Value.Data = pair.edge().STRING(1).GetText().Trim('"').ToDataBlockNodeID();
                            }else if(pair.fedge()!=null)
                            {
                                kv.Key.Data = pair.edge().STRING(0).GetText().Trim('"').ToDataBlockNodeID();
                                kv.Value.Data.Str = pair.edge().STRING(1).GetText().Trim('"');
                            }else if(pair.dedge()!=null)
                            {
                                kv.Key.Data = pair.edge().STRING(0).GetText().Trim('"').ToDataBlockNodeID();
                                kv.Value.Data = pair.edge().STRING(1).GetText().Trim('"').ToDataBlockNodeID();
                            }
                            
                            vs.Add(kv);
                        }
                        
                    }
                    
                    n.Attributes.AddRange(vs);
                    
                    n.Id.Pointer = Utils.NullMemoryPointer();
                    n.Fragments.Add(Utils.NullMemoryPointer());
                    n.Fragments.Add(Utils.NullMemoryPointer());
                    n.Fragments.Add(Utils.NullMemoryPointer());
                    _nodes.Add(n);
                }
                
                // n.Attributes.Add();
                if ((pm & PrintMode.Verbose) != 0)
                {
                    Console.WriteLine($"\nstatus> put({String.Join(", ", _nodes.Select(_=>_.Id.Iri))})");
                }
                // todo move the console stuff to the cli project.
                var sw = Stopwatch.StartNew();
                _adder(_nodes);
                flushed = false;
            }
        }



        internal PrintMode GetPrintMode(AHGHEEParser.FlagsContext fc)
        {
            var fgs = fc?.GetText() ?? "";
            PrintMode pm = PrintMode.Simple;
 
            if (fgs.Any(f => f == 'h')) pm |= PrintMode.History;
            if (fgs.Any(_=> _ == 't')) pm |= PrintMode.Times;
            if (fgs.Any(_ => _ == 'v')) pm |= PrintMode.Verbose;
            return pm;
        }
        void getNodes(IEnumerable<NodeID> ab, PrintMode pm, Step pipes)
        {
             if ((pm & PrintMode.Verbose) != 0)
             {
                 Console.WriteLine($"\nstatus> get({string.Join("\n,", ab.Select(_ => _.Iri))})");
             }

             var sw = Stopwatch.StartNew();
             _query(ab, pipes);

        }

        public override void ExitGet(AHGHEEParser.GetContext context)
        {
            try
            {
                var pm = GetPrintMode(context.flags());       
                
                
                if (!flushed && ((PrintMode.Verbose & pm) != 0))
                {
                    Console.WriteLine($"\nstatus> flushing writes (todo: cmd autoflush false to disable)");
                    _flush();
                    flushed = true;
                }
                
                var nodeidContexts = context.nodeid().ToList();
                
                var nodesToGet = nodeidContexts.Select(nodeIdContext =>
                {
                    return nodeIdContext.ToNodeID();
                }).ToList();

                var pipes = context.pipe()?.ToPipeFlow();
                
                getNodes(nodesToGet, pm, pipes);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
    public class CommandVisitor : AHGHEEBaseVisitor<bool>{
        public bool VisitCommand(AHGHEEParser.CommandContext context){
            return true;
        }
    }

    public class ErrorListener : BaseErrorListener
    {
        
    }
    
    public partial class AHGHEEBaseVisitor<Result>
    {
        
    }
}