using Antlr4.Runtime;
using System.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Ahghee;
using Ahghee.Grpc;
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
            foreach(var nc in context.json()){
                
                var n = JsonParser.Default.Parse<Node>( nc.GetText() );
                n.Id.Pointer = Utils.NullMemoryPointer();
                n.Fragments.Add(Utils.NullMemoryPointer());
                n.Fragments.Add(Utils.NullMemoryPointer());
                n.Fragments.Add(Utils.NullMemoryPointer());
               // n.Attributes.Add();
                Console.WriteLine($"\nstatus> put({n.Id.Iri})");
                var sw = Stopwatch.StartNew();
                var adding = _store.Add(new [] {n}).ContinueWith(adding =>
                {
                    if (adding.IsCompletedSuccessfully)
                    {
                        sw.Stop();
                        Console.WriteLine($"\nstatus> put({n.Id.Iri}).done in {sw.ElapsedMilliseconds}ms");
                    }
                    else
                    {
                        Console.WriteLine($"\nstatus> put({n.Id.Iri}).err({adding?.Exception?.InnerException?.Message})");
                    }
                    Console.Write("\nwat> ");
                });
                flushed = false;
            }
        }

        public override void ExitGet(AHGHEEParser.GetContext context)
        {
            if (!flushed)
            {
                Console.WriteLine($"\nstatus> flushing writes (todo: cmd autoflush false to disable)");
                _store.Flush();
                flushed = true;
            }
            void getNodes(IEnumerable<NodeID> ab)
            {
                Console.WriteLine($"\nstatus> get({string.Join("\n,", ab.Select(_ => _.Iri) )})");
                var sw = Stopwatch.StartNew();
                var t = _store.Items(ab)
                    .ContinueWith(get =>
                    {
                        try
                        {

                        

                            sw.Stop();
                            if (get.IsCompletedSuccessfully)
                            {
                                foreach (var result in get.Result)
                                {
                                    if (result.Item2 is Either<Node, Exception>.Left _n)
                                    {
                                        Console.WriteLine(
                                            $"\nstatus> get({result.Item1.Iri}).done\n{JsonFormatter.Default.Format(_n.Item)}");
                                    }

                                    ;
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
            };

            try
            {
                var ids = context.nodeid().ToList();
                var ab = ids.Select(id =>
                {
                    var json = id.obj();
                    
                    var text = json.GetText();
                    var ab = Google.Protobuf.JsonParser.Default.Parse<NodeID>(text);
                    ab.Pointer = Utils.NullMemoryPointer();
                    return ab;
                
                    // var dburi = id.graphid();
                    //
                    //
                    // var ub = new UriBuilder(dburi.id().GetText());
                    // var ac = new NodeID
                    // {
                    //     Iri = ub.Uri.ToString(),
                    //     Pointer = Utils.NullMemoryPointer()
                    // };
                    // if (dburi.remote() != null)
                    // {
                    //     ac.Remote = dburi.remote().GetText();
                    // }
                    // return ac;
                }).ToList();
                getNodes(ab);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            

        }

        public override void ExitGetf(AHGHEEParser.GetfContext context){
            foreach(var id in context.nodeid()){
                Console.WriteLine("getf a nodeid: " + id);
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