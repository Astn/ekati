using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Ahghee;
using Ahghee.Grpc;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace server
{
    public class WatService : Ahghee.Grpc.WatDbService.WatDbServiceBase
    {
        private readonly ILogger<WatService> _logger;
        private readonly IStorage _db;
        private TimeSpan _procTotalProcessorTime;
        private DateTime _dateTime;
        private Process _proc;

        public WatService(ILogger<WatService> logger, IStorage db)
        {
            _logger = logger;
            _db = db;

            // these are used to keep track of cpu.
            _proc = Process.GetCurrentProcess();
            _procTotalProcessorTime = _proc.TotalProcessorTime;
            _dateTime = DateTime.UtcNow;
        }

        public override async Task<GetMetricsResponse> GetMetrics(GetMetricsRequest request, ServerCallContext context)
        {
            return await _db.GetMetrics(request,context.CancellationToken);
            var cpuPercent = UpdateCpuPercent();
        }

        private float UpdateCpuPercent()
        {
            var newnow = DateTime.UtcNow;
            var elapsed = (newnow - _dateTime).TotalMilliseconds;
            var newcpu = _proc.TotalProcessorTime;
            var cputime = (newcpu - _procTotalProcessorTime).TotalMilliseconds;
            var cpuPercent = Convert.ToSingle(cputime / (Environment.ProcessorCount * elapsed));
            _dateTime = newnow;
            _procTotalProcessorTime = newcpu;
            return cpuPercent;
        }

        public override async Task Get(Query request, IServerStreamWriter<Node> responseStream, ServerCallContext context)
        {
            try
            {
                // todo: pass this into the Items call. context.CancellationToken;
                var resutl = await _db.Items(request.Iris.Select(iri => new NodeID
                {
                    Iri = iri,
                    Remote = "",
                    Pointer = Utils.NullMemoryPointer()
                }), request.Step);

                foreach (var chunk in resutl)
                {
                    var (z, b) = chunk;
                    if (b.IsLeft)
                    {
                        await responseStream.WriteAsync(b.left);
                    }
                    else
                    {
                        _logger.LogError(b.right, "Failure processing query");
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e,"Get failed");
            }
        }

        public override async Task<PutResponse> Put(Node request, ServerCallContext context)
        {
            try
            {
                await _db.Add(new[] {request});
                return new PutResponse{Success = true};
            }
            catch (Exception e)
            {
                _logger.LogError(e,"Put failed");
                return new PutResponse{Success = false};
            }
        }

        public override async Task<GetStatsResponse> GetStats(GetStatsRequest request, ServerCallContext context)
        {

            return await _db.GetStats(request, context.CancellationToken);
            
        }

        public override Task ListPolicies(ListPoliciesRequest request, IServerStreamWriter<Node> responseStream, ServerCallContext context)
        {
            return base.ListPolicies(request, responseStream, context);
        }

        public override Task<ListStatsResponse> ListStats(ListStatsRequest request, ServerCallContext context)
        {
            return base.ListStats(request, context);
        }
    }
}
