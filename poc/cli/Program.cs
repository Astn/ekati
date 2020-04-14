using System;
using System.IO;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Antlr4.Runtime;
using cli.antlr;
using Ahghee;
using App.Metrics;
using Microsoft.AspNetCore.Identity;
using Microsoft.FSharp.Core;
using Unit = Microsoft.FSharp.Core.Unit;

namespace cli
{
    class Program
    {

        static string test1 = @"
put {""id"":{""iri"":""wat/1""},""attributes"":[{""key"": {""Data"":{""str"":""hi""}},""value"":{""Data"":{""str"":""wat""}}}]}
get {""iri"":""wat/1""}

put {""id"":{""iri"":""wat/1""},""attributes"":[{""key"": {""Data"":{""str"":""bye""}},""value"":{""Data"":{""str"":""watter""}}}]} 
get {""iri"":""wat/1""}
get wat/1

put {""id"":{""iri"":""wat/2""},""attributes"":[{""key"": {""Data"":{""str"":""hi""}},""value"":{""Data"":{""str"":""bat""}}}]}
put {""id"":{""iri"":""wat/2""},""attributes"":[{""key"": {""Data"":{""str"":""bye""}},""value"":{""Data"":{""str"":""batter""}}}]} 
get {""iri"":""wat/2""}
get wat/2
get wat/1
        ";        
        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting up...");

            var store = new Ahghee.GrpcFileStore(new Config(
                 Convert.ToInt32( Environment.ProcessorCount * .75),
                FSharpFunc<string, Unit>.FromConverter(
                    input => { return null; }),
                false,
                AppMetrics
                    .CreateDefaultBuilder()
                    .Build())) as IStorage;

            var ms = new MemoryStream();
            var sw = new StreamWriter(ms);
            sw.Write(test1);
            sw.Flush();
            ms.Seek(0, SeekOrigin.Begin);
            //ms.Seek(0, SeekOrigin.Begin);
            var instr = new StreamReader(ms);
            

            //TextReader tx
            var ins  = new Antlr4.Runtime.AntlrInputStream(instr);
            var lex = new AHGHEELexer(ins);
            
            var parser = new AHGHEEParser(new BufferedTokenStream(lex));
            parser.BuildParseTree = true;
            parser.AddParseListener(listener: new Listener(store));
            parser.AddErrorListener(new ErrorListener());
            AHGHEEParser.CommandContext cc = null;
            
            for (; ; cc = parser.command())
            {
                
                
                Console.Write("wat> ");
                if(cc?.exception!=null 
                   //&& cc.exception.GetType() != typeof(Antlr4.Runtime.InputMismatchException)
                   //&& cc.exception.GetType() != typeof(Antlr4.Runtime.NoViableAltException)
                   ){
                    Console.WriteLine(cc.exception.Message);
                    Console.WriteLine($"found {cc.exception.OffendingToken.Text} at Line {cc.exception.OffendingToken.Line} offset at {cc.exception.OffendingToken.StartIndex}");
                }
                await Task.Delay(30);
                var str = "";
                var pos = ms.Position;
                do
                {
                    if (!String.IsNullOrWhiteSpace(str))
                    {
                        sw.Write(str);
                    }
                    str = Console.ReadLine();
                } while (str != "!");
                sw.Flush();
                ms.Seek(pos, SeekOrigin.Begin);
            }
        }
    }
}