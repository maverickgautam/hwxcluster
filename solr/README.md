# Solr


============================================================================================================================

collection is made of => document is made of=> fields




                                        Collection
                                           |
                                           |
                                           |
                                      route/hashing
                                           |
                                           |
                                =========================
                                |           |            |
                             shard1       shard2       shard3








                            ================================
                            |              NODE             |
                            |                               |
                            |  ===========    ==========    |
                            | |           |  |          |   |
                            | |           |  |          |   |
                            | |   Core1   |  | Core2    |   |
                            | |           |  |          |   |
                            |  ===========    ==========    |
                            |                               |
                            |                               |
                            ================================

   1. Collection is distributed accross Node.
   2. Collection is divided into shards.
   3. A shard is replicated across nodes.
   4. A shard is indexed and known as core. (A replication also has index and hence it also has a core)
   5. Node is the JVM running Solr instances.
   6. A Node can have multiple Cores.

============================================================================================================================

solr related links

http://www.nridge.com/presentations/ApacheSolrPresentation.pdf
https://doc.lucidworks.com/lucidworks-hdpsearch/2.3/Guide-Solr.html
https://cwiki.apache.org/confluence/display/solr/Nodes%2C+Cores%2C+Clusters+and+Leaders

