using Antlr4.Runtime;
using System.IO;
using System;
namespace cli.antlr
{
    public class Listener : AHGHEEBaseListener
    {
        public override void ExitPut(AHGHEEParser.PutContext context){
            foreach(var n in context.node()){
                Console.WriteLine("put a node: " + n);
            }
        }

        public override void ExitGet(AHGHEEParser.GetContext context){
            foreach(var id in context.nodeid()){
                Console.WriteLine("get a nodeid: " + id);
            }
        }

        public override void ExitGetf(AHGHEEParser.GetfContext context){
            foreach(var id in context.nodeid()){
                Console.WriteLine("getf a nodeid: " + id);
            }
        }
    }

    public class CommandVisitor : AHGHEEBaseVisitor<bool>{
        public override bool VisitCommand(AHGHEEParser.CommandContext context){
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