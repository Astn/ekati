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
using Ekati;
using Ekati.Core;
using Antlr4.Runtime.Tree;
using Ekati.Ext;
using FlatBuffers;
using parser_grammer;

namespace cli.antlr
{
    public class Listener : AHGHEEBaseListener
    {
       //private readonly IStorage _store;
        private readonly Func<IEnumerable<Node>, Task> _adder;
        private bool flushed = false;
        private readonly Action<IEnumerable<NodeID>> _query;
        private readonly Action _flush;
        private readonly Action<string,string> _load;

        public Listener(Func<IEnumerable<Node>,Task> adder, Action<IEnumerable<NodeID>> query, Action flush, Action<string, string> load)
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
                    // this first case we are an anon node. we generate an iri based on the time
                    if (nc.obj() != null)
                    {
                        if (nc.obj().kvps() == null)
                        {
                            Console.WriteLine($"testFor kvps: {nc.obj().kvps().GetText()}");
                        }
                        // todo: can we determine our buffer size based on info in the node context?
                        var builder = new FlatBufferBuilder(48);
                        var m = nc.obj().kvps().ToMap(builder);
                        var node = Utils.Nodee(builder, Utils.Id(builder, "", $"epoc{DateTime.UtcNow.ToFileTime()}"), m);
                        Node.FinishNodeBuffer(builder,node);
                        var asNode = Node.GetRootAsNode(builder.DataBuffer);
                        _nodes.Add(asNode);
                        Console.WriteLine($"Added: {asNode.ToString()}");
                    }
                    else if(nc.nodeid()!= null && nc.kvps() != null)
                    {
                        
                        if (nc.nodeid().obj() != null)
                        {
                            throw new NotImplementedException();
                            //nc.nodeid().obj().kvps().pair().Where(p => p.kvp().)
                        }
                        else
                        {
                            var remote = nc.nodeid().remote() != null ? nc.nodeid().remote().GetText().Trim('"') : "";
                            var iri = nc.nodeid().id().GetText().Trim('"');
                            // todo: can we determine our buffer size based on info in the node context?
                            var builder = new FlatBufferBuilder(48);
                            var m = nc.kvps().ToMap(builder);
                            var node = Utils.Nodee(builder, Utils.Id(builder, remote, iri), m);
                            Node.FinishNodeBuffer(builder,node);
                            var asNode = Node.GetRootAsNode(builder.DataBuffer);
                            _nodes.Add(asNode);
                        }
                    }
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
        void getNodes(IEnumerable<NodeID> ab, PrintMode pm)
        {
             if ((pm & PrintMode.Verbose) != 0)
             {
                 Console.WriteLine($"\nstatus> get({string.Join("\n,", ab.Select(_ => _.Iri))})");
             }

             var sw = Stopwatch.StartNew();
             _query(ab);

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
                    var builder = new FlatBufferBuilder(16);
                    var nid = nodeIdContext.ToNodeID(builder);
                    builder.Finish(nid.Value);
                    return NodeID.GetRootAsNodeID(builder.DataBuffer);
                }).ToList();

                getNodes(nodesToGet, pm);
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