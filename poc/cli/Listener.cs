using Antlr4.Runtime;
using System.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Ahghee;
using Ahghee.Grpc;
using Antlr4.Runtime.Tree;
using cli_grammer;
using Google.Protobuf;


namespace cli.antlr
{
    public class Listener : AHGHEEBaseListener
    {
        private readonly IStorage _store;
        private bool flushed = false;
        public Listener(IStorage store)
        {
            _store = store;
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
                            n.Id.Remote = nc.nodeid().remote().GetText();
                        }

                        n.Id.Iri = nc.nodeid().id().GetText();
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
                            kv.Value.Data = pair.value().ToDataBlock();
                            kv.Key.Data.Str = pair.STRING().GetText();
                            

                            // handle json array or json object as typebytes
                            
                            
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
                var sw = Stopwatch.StartNew();
                var adding = _store.Add(_nodes).ContinueWith(adding =>
                {
                    if (adding.IsCompletedSuccessfully)
                    {
                        sw.Stop();
                        Console.WriteLine($"\nstatus> put({String.Join(", ", _nodes.Select(_=>_.Id.Iri))}).done in {sw.ElapsedMilliseconds}ms");
                    }
                    else
                    {
                        Console.WriteLine($"\nstatus> put({String.Join(", ", _nodes.Select(_=>_.Id.Iri))}).err({adding?.Exception?.InnerException?.Message})");
                    }
                    Console.Write("\nwat> ");
                });
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
             var t = _store.Items(ab, pipes)
                 .ContinueWith(get =>
                 {
                     try
                     {
                         sw.Stop();
                         if (get.IsCompletedSuccessfully)
                         {
                             var sb = new StringBuilder();
                             foreach (var result in get.Result)
                             {
                                 if (result.Item2 is Either<Node, Exception>.Left _n)
                                 {
                                     sb.Append("\nstatus> get(");
                                     sb.Append(result.Item1.Iri);
                                     sb.Append(").done");

                                     sb.NodePrinter(_n.Item,0,pm);
                                     sb.Clear();
                                 }

                                 if (result.Item2 is Either<Node, Exception>.Right _e)
                                 {
                                     Console.WriteLine($"\nstatus> get({result.Item1.Iri}).err({_e.Item.Message})");
                                 }
                             }

                             Console.WriteLine($"status> completed in {sw.ElapsedMilliseconds}ms");
                         }
                         else
                         {
                             Console.WriteLine($"\nstatus> get(...).err({get?.Exception?.InnerException?.Message})");
                         }

                         Console.Write("\nwat> ");
                     }
                     catch (Exception e)
                     {
                         Console.WriteLine(e);
                         throw;
                     }
                 });
        }

        public override void ExitGet(AHGHEEParser.GetContext context)
        {
            try
            {
                var pm = GetPrintMode(context.flags());       
                
                
                if (!flushed && ((PrintMode.Verbose & pm) != 0))
                {
                    Console.WriteLine($"\nstatus> flushing writes (todo: cmd autoflush false to disable)");
                    _store.Flush();
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