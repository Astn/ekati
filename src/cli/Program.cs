using System;
using System.Collections.Generic;
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
using Ekati;
using Ekati.Grpc;
using App.Metrics;
using Ekati.Core;
using Microsoft.AspNetCore.Identity;
using Microsoft.FSharp.Core;
using Unit = Microsoft.FSharp.Core.Unit;

namespace cli
{
    class Program
    {
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
                var filename = Ekati.Program.benchmark(1000, 2);
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

            //
            // Console.WriteLine("Starting up...");
            //
            // var store = new Ahghee.GrpcFileStore(new Config(
            //      Convert.ToInt32( 1 ), //Environment.ProcessorCount * .75),
            //     FSharpFunc<string, Unit>.FromConverter(
            //         input => { return null; }),
            //     false,
            //     AppMetrics
            //         .CreateDefaultBuilder()
            //         .Build())) as IStorage;

        }
    }
}