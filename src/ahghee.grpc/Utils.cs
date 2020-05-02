using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ahghee.Grpc
{
    public static class Utils
    {
        public static void IntoSpan(this int number, Span<byte> dest)
        {
            var sizeBytes = BitConverter.GetBytes(number);
            for (var i = 0; i < sizeBytes.Length; i++)
            {
                dest[i] = sizeBytes[i];
            } 
        }
        public static MemoryPointer NullMemoryPointer()
        {
            var mp = new MemoryPointer
            {
                Filename = 0,
                Length = 0,
                Offset = 0,
                Partitionkey = 0
            };
            return mp;
        }
    }

    public readonly struct Either<A, B>
    {
        public readonly bool IsLeft;
        public bool IsRight => !IsLeft;
        public readonly A Left;
        public readonly B Right;
        public Either(A left)
        {
            IsLeft = true;
            Left = left;
            Right = default;
        }

        public Either(B right)
        {
            IsLeft = false;
            Left = default;
            Right = right;
        }
    }
    
    public interface IStorage
    {
        IEnumerable<Node> Nodes();
        void Flush();
        Task Add(IEnumerable<Node> nodes);
        Task Remove(IEnumerable<NodeID> nodes);
        Task<IEnumerable<(NodeID , Either<Node, Exception>)>> Items(IEnumerable<NodeID> nodes, Step step);
        Task<Node> First(Func<Node, bool> find);
        void Stop();
        Task<GetStatsResponse> GetStats(GetStatsRequest req, CancellationToken cancel);
        Task<GetMetricsResponse> GetMetrics(GetMetricsRequest req, CancellationToken cancel);

    }
    

}