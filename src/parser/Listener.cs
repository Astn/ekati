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
        private readonly Action<string,string> _load;

        public Listener(Func<IEnumerable<Node>,Task> adder, Action<IEnumerable<NodeID>, Step> query, Action flush, Action<string, string> load)
        {
            // _store = store;
            _adder = adder;
            _query = query;
            _flush = flush;
            _load = load;
        }

        public override void ExitLoad(AHGHEEParser.LoadContext context)
        {
            var strType = context.loadtype().GetText();
            var strPath = context.loadpath().GetText();
            _load(strType, strPath);
        }

        public override void ExitPut(AHGHEEParser.PutContext context){
            Console.WriteLine($"Found a put .. : {context.GetText()}");
            var pm = GetPrintMode(context.flags());  
            foreach(var nc in context.node()){

                
                try
                {
                    Console.WriteLine($"Found a node.. : {nc.GetText()}");
                
                
                    List<Node> _nodes = new List<Node>();
                    if (nc.obj() != null)
                    {
                        if (nc.obj().kvps() == null)
                        {
                            Console.WriteLine($"testFor kvps: {nc.obj().kvps().GetText()}");
                        }
                        var m = nc.obj().kvps().ToMap();
                        var n = new Node();
                        n.Id = new NodeID();
                        n.Id.Iri = m.Attributes.FirstOrDefault(a => a.Key?.Data?.Str != null)?.Key.Data.Str ?? $"epoc{DateTime.UtcNow.ToFileTime()}" ;
                        n.Id.Pointer = Utils.NullMemoryPointer();
                        n.Fragments.Add(Utils.NullMemoryPointer());
                        n.Fragments.Add(Utils.NullMemoryPointer());
                        n.Fragments.Add(Utils.NullMemoryPointer());
                        _nodes.Add(n);
                        Console.WriteLine($"Added: {n.ToString()}");
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

                        if (nc.kvps() != null)
                        {
                            var m = nc.kvps().ToMap();
                            
                            n.Attributes.AddRange(m.Attributes);
                        }
                        
                        n.Id.Pointer = Utils.NullMemoryPointer();
                        n.Fragments.Add(Utils.NullMemoryPointer());
                        n.Fragments.Add(Utils.NullMemoryPointer());
                        n.Fragments.Add(Utils.NullMemoryPointer());
                        _nodes.Add(n);
                    }
                    
                    // n.Attributes.Add();
                   // if ((pm & PrintMode.Verbose) != 0)
                    {
                        Console.WriteLine($"\nstatus> put({String.Join(", ", _nodes.Select(_=>_.Id.Iri))})");
                    }
                    // todo move the console stuff to the cli project.
                    _adder(_nodes);
                    flushed = false;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

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