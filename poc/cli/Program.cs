using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
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

get {""iri"":""wat/1""}
get ""wat/2"", ""wat/1"" |> filter ""str"" ==  ""watter"" 
put austin 
    ""name"":""Austin"",
    ""age"": 38,  
    ""child"":@""gwynneth"",
    ""child"":@""august"",
    ""child"":@""blakely"",
    ""spouce"":@""kendra"";
    kendra 
    ""name"":""Kendra"", 
    ""age"": 32,    
    ""child"":@""gwynneth"",
    ""child"":@""august"",
    ""child"":@""blakely"",
    ""spouce"":@""austin"";
    gwynneth 
    ""name"":""Gwynneth"", 
    ""age"": 5,    
    ""mother"":@""kendra"",
    ""brother"":@""august"",
    ""sister"":@""blakely"",
    ""father"":@""austin"";
    blakely 
    ""name"":""Blakely"", 
    ""age"": 3,    
    ""mother"":@""kendra"",
    ""brother"":@""august"",
    ""sister"":@""gwynneth"",
    ""father"":@""austin"";
    august 
    ""name"":""August"", 
    ""age"": 2,
    ""mother"":@""kendra"",
    ""sister"":@""blakely"",
    ""sister"":@""gwynneth"",
    ""father"":@""austin"";
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
            void RenderReport(string metricsFile)
            {
                Console.WriteLine("Generating Report...");
                var sb = new StringBuilder();
                report.Program.main(metricsFile, sb);
                var reportFile = $"./report.{DateTime.Now.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture)}.html";
                File.WriteAllText(reportFile, sb.ToString());
                var fi = new FileInfo(reportFile);
                Console.WriteLine($"Finished generating report at\nstart {fi.FullName}");
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    Process.Start(new ProcessStartInfo("cmd", $"/c start {fi.FullName}") { CreateNoWindow = true });
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    Process.Start("xdg-open", fi.FullName);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                {
                    Process.Start("open", fi.FullName);
                }
            }
            
            if (args.Any(a => a == "benchmark"))
            {
                Console.WriteLine("Starting benchmark...");
                var filename = Ahghee.Program.benchmark(1000, 2);
                Console.WriteLine("Finished benchmark...");
                Console.WriteLine($"Metrics files created at {filename}");
                RenderReport(filename);
                return;
            }

            if (args.Any(a => a == "report"))
            {
                RenderReport(args.First(a => a != "report"));
                return;
            }
            
            Console.WriteLine("Starting up...");

            var store = new Ahghee.GrpcFileStore(new Config(
                 Convert.ToInt32( 1 ), //Environment.ProcessorCount * .75),
                FSharpFunc<string, Unit>.FromConverter(
                    input => { return null; }),
                false,
                AppMetrics
                    .CreateDefaultBuilder()
                    .Build())) as IStorage;

            using var disposableStore = (IDisposable)store;
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

            //TextReader tx

        }
    }
}