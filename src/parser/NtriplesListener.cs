using System;
using System.Collections.Generic;
using Ekati;
using Ekati.Core;
using FlatBuffers;
using parser_grammer;

namespace cli.antlr
{
    public class NtriplesListener : NTRIPLESBaseListener
    {
        private readonly Action<Node> _gotNode;
        Dictionary<string,string> BNs = new Dictionary<string, string>();
        private readonly string BNRoot = DateTime.UtcNow.ToString("u").Replace(" ",":").Replace(":",".");
        public NtriplesListener(Action<Node> gotNode)
        {
            _gotNode = gotNode;
        }

        private string BNToId(string bn)
        {
            if (BNs.ContainsKey(bn))
            {
                return BNs[bn];
            }
    
            //substring off the _:
            var genIRI = $"blank:{BNRoot}:{bn.Substring(2)}";
            BNs[bn] = genIRI;
            return genIRI;
        }
        public override void ExitTriple(NTRIPLESParser.TripleContext context)
        {
            try
            {
                var builder = new FlatBufferBuilder(32);
                var nodeId = context.subj().ToNodeId(builder, BNToId);

                var pred = context.pred().ToDataBlock(builder);

                var obj = context.obj().ToDataBlock(builder, BNToId);

                var kv = KeyValue.CreateKeyValue(builder, 0L, Utils.Tmd(builder, pred), obj);
                var kvArr = Map.CreateItemsVector(builder, new[] {kv});

                var node = Utils.Nodee(builder, nodeId, Map.CreateMap(builder, kvArr));
                Node.FinishNodeBuffer(builder, node);
                
                _gotNode(Node.GetRootAsNode(builder.DataBuffer));
            }
            catch (Exception )
            {
                Console.WriteLine($"Uh.. got this -->\n{context.GetText()}\n<-- Ends here");
                throw;
            }
        }
    }
}