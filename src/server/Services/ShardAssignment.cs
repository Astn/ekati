using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using DotNext.IO;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Messaging;
using Google.Protobuf;
using Ekati.Protocol.Grpc;
using IMessage = DotNext.Net.Cluster.Messaging.IMessage;

namespace server
{
    public class ShardAssignment
    {
        public static readonly string Name = "ShardAssignment";
        public int Shards { get; set; }
        public string[] Leaders { get; set; }
        public string[][] Followers { get; set; }

        public ShardAssignment(int shards, IReadOnlyCollection<IClusterMember> readOnlyCollection)
        {
            this.Shards = shards;
            Leaders = new string[shards];
            var membersSorted = readOnlyCollection.OrderBy(x => x.Id);
            var members = readOnlyCollection.Count;
            for (int i = 0; i < shards; i++)
            {
                Leaders[i] = readOnlyCollection.ToArray()[i % members].Id.ToString();
            }
            // make a queue that contains each cluster member twice in random order
            var rand = new Random();
            var eachMemberTwice = readOnlyCollection.ToList();
            eachMemberTwice.AddRange(readOnlyCollection.ToList());
            var followerAssignments = new Queue<string>();
            foreach (var clusterMember in eachMemberTwice.OrderBy(x => rand.Next()))
            {
                followerAssignments.Enqueue(clusterMember.Id.ToString());
            }

            // no followers if only one member
            if (members == 1)
            {
                return;
            }
            
            // assign each item in the queue to be a follower as long as its not the leader for that same item
            for (int i = 0; i < shards; i++)
            {
                Followers[i] = new string[2];
                var j = 0;
                // first follower
                while (true)
                {
                    var item = followerAssignments.Dequeue();
                    if (item != Leaders[i])
                    {
                        Followers[i][j] = item;
                        break;
                    }
                    followerAssignments.Enqueue(item);
                }

                // if we have less than 3 followers, then we cant add a 2nd follower
                if (members < 3)
                {
                    continue;
                }
                // second follower
                while (true)
                {
                    var item = followerAssignments.Dequeue();
                    if (item != Leaders[i] && item != Followers[i][j])
                    {
                        Followers[i][++j] = item;
                        break;
                    }
                    followerAssignments.Enqueue(item);
                }
                
            }
        }

        public ShardAssignment(Ekati.Protocol.Grpc.ShardAssignment other)
        {
            InitFrom(other);
        }

        public ShardAssignment(IMessage clusterMessage): base()
        {
            if (clusterMessage.Name == "GetShardAssignment")
            {
                var respBytes = clusterMessage.ToByteArrayAsync().Result;
                var respSA = new Ekati.Protocol.Grpc.ShardAssignment();
                respSA.MergeFrom(respBytes);
                InitFrom(respSA);
            }
        }

        public Ekati.Protocol.Grpc.ShardAssignment ToGrpcShardAssignment()
        {
            return ToGrpcShardAssignment(this);
        }
        public void InitFrom(Ekati.Protocol.Grpc.ShardAssignment other)
        {
            Leaders = other.Leaders.ToArray();
            
            Followers = new string[other.Followers.Count][];
            for (int i = 0; i < other.Followers.Count; i++)
            {
                Followers[i] = other.Followers[i].Follower.ToArray();
            }
        }
        public static Ekati.Protocol.Grpc.ShardAssignment ToGrpcShardAssignment(ShardAssignment sa)
        {
            var respSA = new Ekati.Protocol.Grpc.ShardAssignment();
            respSA.Leaders.AddRange(sa.Leaders);
            for (int i = 0; i < sa.Followers.GetLength(0); i++)
            {
                var fo = new Followers();
                fo.Follower.AddRange(sa.Followers[i]);
                respSA.Followers.Add(fo);
            }

            return respSA;
        }

        public BinaryMessage ToBinaryMessage()
        {
            var grpc = this.ToGrpcShardAssignment();
            var bytes = grpc.ToByteArray();
            var ros = new ReadOnlySequence<byte>(bytes);
            return new BinaryMessage(ros, "GetShardAssignment");
        }
    }
}