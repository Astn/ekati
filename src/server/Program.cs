using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using DotNext.Net.Cluster.Consensus.Raft.Http.Hosting;

namespace server
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        // Additional configuration is required to successfully run gRPC on macOS.
        // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureKestrel(options =>
                    {
                        // Unable to start ASP.NET Core gRPC app on macOS
                        // Kestrel doesn't support HTTP/2 with TLS on macOS and older Windows versions such as Windows 7.
                        // The ASP.NET Core gRPC template and samples use TLS by default.
                        // You'll see the following error message when you attempt to start the gRPC server:
                        
                        // Setup a HTTP/2 endpoint without TLS.
                        //options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
                    });
                    webBuilder.UseStartup<Startup>();
                }).JoinCluster("Raft");
    }
}
