using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Ahghee.Grpc;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace server
{
    public class WatService : Ahghee.Grpc.WatDbService.WatDbServiceBase
    {
        private readonly ILogger<WatService> _logger;
        private TimeSpan _procTotalProcessorTime;
        private DateTime _dateTime;
        private Process _proc;

        public WatService(ILogger<WatService> logger)
        {
            _logger = logger;
            
            // these are used to keep track of cpu.
            _proc = Process.GetCurrentProcess();
            _procTotalProcessorTime = _proc.TotalProcessorTime;
            _dateTime = DateTime.UtcNow;
        }

        public override async Task<GetMetricsResponse> GetMetrics(GetMetricsRequest request, ServerCallContext context)
        {
            var cpuPercent = UpdateCpuPercent();
            var memMB = _proc.WorkingSet64 * 0.000001f;
            return new GetMetricsResponse
            {
                Metrics =
                {
                    new GetMetricsResponse.Types.Metric{Name = "Cpu", Value = cpuPercent, Time = Timestamp.FromDateTimeOffset(DateTimeOffset.Now)},
                    new GetMetricsResponse.Types.Metric{Name = "Mem", Value = memMB, Time = Timestamp.FromDateTimeOffset(DateTimeOffset.Now)},
                    new GetMetricsResponse.Types.Metric{Name = "Disk", Value = 00.1f, Time = Timestamp.FromDateTimeOffset(DateTimeOffset.Now)},
                    new GetMetricsResponse.Types.Metric{Name = "Network", Value = 00.1f, Time = Timestamp.FromDateTimeOffset(DateTimeOffset.Now)},
                }
            };
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

        public override Task Get(Query request, IServerStreamWriter<Node> responseStream, ServerCallContext context)
        {
            return base.Get(request, responseStream, context);
        }

        public override Task<PutResponse> Put(Node request, ServerCallContext context)
        {
            return base.Put(request, context);
        }

        public override Task<GetStatsResponse> GetStats(GetStatsRequest request, ServerCallContext context)
        {
            return base.GetStats(request, context);
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
