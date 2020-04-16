using System;
using System.IO;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Antlr4.Runtime;
using cli.antlr;
using Ahghee;
using App.Metrics;
using cli_grammer;
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
get {""iri"":""wat/1""}

put {""id"":{""iri"":""wat/2""},""attributes"":[{""key"": {""Data"":{""str"":""hi""}},""value"":{""Data"":{""str"":""bat""}}}]}
put {""id"":{""iri"":""wat/2""},""attributes"":[{""key"": {""Data"":{""str"":""bye""}},""value"":{""Data"":{""str"":""batter""}}}]} 
get {""iri"":""wat/2""}
get {""iri"":""wat/2""}
get {""iri"":""wat/1""}
        ";
        
        static UnbufferedTokenStream makeStream(string text)
        {
            var sr = new StringReader(text);
            var ins  = new AntlrInputStream(sr);
            var lex = new AHGHEELexer(ins);
            return new UnbufferedTokenStream(lex);
        }
        
        static UnbufferedTokenStream makeStream(TextReader reader)
        {
            var ins  = new AntlrInputStream(reader);
            var lex = new AHGHEELexer(ins);
            return new UnbufferedTokenStream(lex);
        }
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

          //TextReader tx
            var parser = new AHGHEEParser(makeStream(test1));
            parser.BuildParseTree = true;
            parser.AddParseListener(listener: new Listener(store));
            parser.AddErrorListener(new ErrorListener());
            AHGHEEParser.CommandContext cc = null;

            for (;; cc = parser.command())
            {
                if (cc?.exception != null
                    //&& cc.exception.GetType() != typeof(Antlr4.Runtime.InputMismatchException)
                    //&& cc.exception.GetType() != typeof(Antlr4.Runtime.NoViableAltException)
                )
                {
                    Console.WriteLine(cc.exception.Message);
                    Console.WriteLine(
                        $"found {cc.exception.OffendingToken.Text} at Line {cc.exception.OffendingToken.Line} offset at {cc.exception.OffendingToken.StartIndex}");
                }

                // we got no more, so jump into console input
                if (parser.CurrentToken.Type == TokenConstants.Eof)
                {
                    Console.Write("wat> ");
                    await Task.Delay(30);
                    var line = await Console.In.ReadLineAsync();
                    if (line != null)
                    {
                        parser.SetInputStream(makeStream(line));
                    }
                }
            }
        }
    }
}