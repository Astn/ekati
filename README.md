# Ekáti (εκάτη / Hecate)
## Ekati - database 

![Basic UI](https://github.com/astn/ekati/workflows/Build/badge.svg)


## TLDR

Under development, lot's to do. If your looking for something production ready, this is NOT it. If you want to get in on an early open source project and have a big impact, this could be your thing.

# Interested? 

Do you like graphs or databases, and solving hard problems? I'm hoping you will help me. 

## But wat is it really?

It's wanting to be a real graph database, not a virtural graph database. You know the ones that are build on top of document databases, or key value stores, or column stores. Not that those are bad, but I'm hoping we can do better. 

The native data representation is flexable in order to support Neo4j / Gremlin style graphs, as well as semantic graphs, or RDF.

There is no smarts built into it as of yet. It dumbly stores and loads data. To solve this I'm working on adding support for webassembly plugins.

## What can it do now?

Right now you can import a few file formats (ttl, graphml) supporting other file types is fairly easy, maybe you want to help add support for one? You would find or create an ANTLR4 grammer, and then add an adapter for that parser to import data.

There is the beginnings of a UI, that runs in your browser.

![Basic UI](/docs/UI-load-graphml.png)

# Tech

 - The UI using a webassembly SPA framework called Blazor.
 - The text editor is the same one that is in VS Code.
 - The pretty data graphs are built using D3.js
 - Communication with the database is done over gRPC.

## Database Tech

 - Mostly written in F# and C#. The code is a fair bit ugly, and needs some refactoring.
 - It's targeting Linux, OSX, and Windows, using the dotnet core JIT.
 - Some indexing is done using RockDB, looking into using  FASTER.
 - The main storage layer from scratch and needs a ton of work. Just starting down the hybrid log structured merge tree approach.
 - There is some sharding support built in, though at this time all the shards have to run on a single machine. Clustering is on the list.
 - Query support is ultra basic, and need to do some work in the query language department.

## Still here?

 - Click *Watch* button
 - Click fork button
 - Jump on Discord https://discord.gg/NfcBmjA

## Good reading
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

## Whiteboard

- [Structure Whiteboard](https://miro.com/app/board/o9J_kz6OZhI=/)

