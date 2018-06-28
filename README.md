# Ahghee - The Big Graph database

[![Build status](https://ci.appveyor.com/api/projects/status/6581it232hdo2qa5?svg=true)](https://ci.appveyor.com/project/Astn/ahghee)

This project is still in it's early early stages, so click that *Watch* button.

I'm looking for other contributors to help.

#### Current State

Under heavy development. Not ready for Production.

Status of the prototype F#
 
 - [x] Protocol Buffers data types
 - [x] Sharding
 - [x] Stores Fragments
 - [x] Links Fragments
 - [x] Resolves Fragment Pointers
 - [x] Entry Index uses RocksDb
 - [x] Single machine ingress tested at 50MB/s and 15m Fragments in 2.5 min. 
 - [ ] Map Reduce
 - [ ] Clustering
 - [ ] Full text index

Status of post prototype Rust

 - [x] Protocol Buffers data types
 - [ ] Stores Fragments
 - [ ] Links Fragments
 - [ ] Resolves Fragment Pointers
 - [ ] Entry Index uses RocksDb
 - [ ] Single machine ingress tested at 50MB/s and 15m Fragments in 2.5 min. 
 - [ ] Map Reduce
 - [ ] Clustering
 - [ ] Full text index

#### Slack

  ahghee.slack.com

#### Contributors 

  Main implementation using Rust in root  
  
  Prototype in FSharp -> see /poc
  
  Start [here](https://github.com/Astn/ahghee/wiki/Getting-Started---Contributors)

## References
- [FASTER](https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf)
- [BigTable](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf)
- [DynamoDB](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Cassandra](https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf)
- [Kafka](http://notes.stephenholiday.com/Kafka.pdf)
- [PebblesDb](http://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf)
- [RocksDb](http://cidrdb.org/cidr2017/papers/p82-dong-cidr17.pdf)
- [Lucene](https://pdfs.semanticscholar.org/2795/d9d165607b5ad6d8b9718373b82e55f41606.pdf)
- [Neo4j](https://neo4j.com/whitepapers/graph-algorithms-optimized-neo4j/)
- [YCSB](https://github.com/brianfrankcooper/YCSB/wiki) 
- [SeaStar](http://docs.seastar.io/master/md_doc_tutorial.html)

## Design goals

- [Structure Whiteboard](https://realtimeboard.com/app/board/o9J_kz6OZhI=/)

- Massive graphs (Trillions of nodes)
- Write friendly (like Cassandra)
- Elastic scaling
- Masterless clustering
- Fast (Millions of graph-node steps per second per server)
- Index-free adjancecy traversal
- Custom indexing
- Automatic adaptive indexing
- Storage local compute
- Large value support
- Standing queries 
- Virtual sub-graph
- Pluggable storage providers
- Pluggable query providers
- Dotnet core embedding
- Cross platform
- Tinkerpop or a variation of Tinkerpop
- Cypher or a variation of Cypher

## Approach
- TDD
- DevOps

### High level strategy
- [Gossip](https://en.wikipedia.org/wiki/Gossip_protocol) for cluster registry
- [gRPC](https://grpc.io/docs/quickstart/csharp.html) for serialization and RPC 
- Use a [Log structured merge approach](http://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf)
- A new cluster-node should be able to join the cluster just by authenticating with any cluster-node
- Gateway nodes should be able to join multiple clusters to form a WAN cluster
- Gateway nodes can control the flow of data between clusters (read/write/one-way)
