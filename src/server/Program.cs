using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using DotNext.Net.Cluster.Consensus.Raft.Http.Hosting;
using Microsoft.AspNetCore.Builder;

namespace server
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var token = new CancellationTokenSource();
            var raftHost = CreateRaftHost(args).Build().RunAsync(token.Token);
            CreateHttpHostBuilder(args).Build().Run();
            token.Cancel();
            raftHost.Wait();
        }

        // Additional configuration is required to successfully run gRPC on macOS.
        // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
        public static IHostBuilder CreateHttpHostBuilder(string[] args) =>
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
                });

        public static IHostBuilder CreateRaftHost(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                    {
                        webBuilder.ConfigureKestrel(options =>
                        {
                            var port = args.First(x => x.StartsWith("--Raft:Port=")).Split('=')[1];
                            options.ListenAnyIP(Convert.ToInt32(port));
                        });
                        webBuilder.UseStartup<RaftStartup>();
                    })
                    .JoinCluster("Raft");
    }

    public class RaftStartup
    {
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
        }
    }
}
