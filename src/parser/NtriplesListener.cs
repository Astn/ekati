using System;
using System.Collections.Generic;
using Ahghee.Grpc;
using cli_grammer;

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
                var nodeId = context.subj().ToNodeId(BNToId);

                var pred = context.pred().ToDataBlock();

                var obj = context.obj().ToDataBlock(BNToId);
                var node = new Node();
                node.Id = nodeId;
                node.Attributes.Add(new KeyValue
                {
                    Key = new TMD
                    {
                        Data = pred,
                    },
                    Value = obj
                });
                _gotNode(node);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Uh.. got this -->\n{context.GetText()}\n<-- Ends here");
                throw;
            }
        }
    }
}