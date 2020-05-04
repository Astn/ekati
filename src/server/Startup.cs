using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ahghee;
using Ahghee.Grpc;
using App.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.FSharp.Core;
using Unit = Microsoft.FSharp.Core.Unit;

namespace server
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddGrpc();

            services.AddSingleton(sp =>
            {
                var lifetimeEvents = sp.GetService<IHostApplicationLifetime>();
                var ekati = new Ahghee.GrpcFileStore(new Config(
                    Convert.ToInt32(1), //Environment.ProcessorCount * .75),
                    FSharpFunc<string, Unit>.FromConverter(
                        input => { return null; }),
                    false,
                    AppMetrics
                        .CreateDefaultBuilder()
                        .Build())) as IStorage;

                lifetimeEvents.ApplicationStopping.Register(() =>
                {
                    Console.WriteLine("Ekati got shutdown event. Calling Stop");
                    ekati.Stop();
                    Console.WriteLine("Ekati got shutdown event. Stop Finished");
                });
                
                return ekati;
            });

            // services.AddGrpcWeb(o => o.GrpcWebEnabled = true);

            // services.AddCors(o =>
            // {
            //     o.AddPolicy("MyPolicy", builder =>
            //     {
            //         builder.WithOrigins("localhost:5001","localhost:5000");
            //         builder.WithMethods("POST,OPTIONS");
            //         builder.AllowAnyHeader();
            //         builder.WithExposedHeaders("Grpc-Status", "Grpc-Message");
            //     });
            // });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseWebAssemblyDebugging();
            }
            else
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            //app.UseHttpsRedirection();
            app.UseBlazorFrameworkFiles();
            app.UseStaticFiles();
            app.UseRouting();
            //
            // app.UseCors("MyPolicy");

            app.UseGrpcWeb();
            
            
            
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<WatService>().EnableGrpcWeb();
                endpoints.MapFallbackToFile("index.html");
                // endpoints.MapGet("/", async context =>
                // {
                //     await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
                // });
            });
        }
    }
}
