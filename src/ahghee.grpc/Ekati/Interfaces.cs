using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ekati.Core
{
    
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
        Task<IEnumerable<Node>> Items(IEnumerable<NodeID> nodes);
        Task<Node> First(Func<Node, bool> find);
        void Stop();
    }

}