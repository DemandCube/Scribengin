#Design#

                                                  ............................................
                                                  .                                          .
     .........................                    .                Zookeeper                 .
     .                       .                    .        Dataflow Worker Coordinator       .
     .        Source         .                    .                                          .
 - - . (HDFS, App, File...)  .                    ............................................
 |   .                       . 
 |   .........................                    ............................................
 |                                                .                                          .
 |                                                .                 Yarn                     .
 |   .........................                    .                                          .
 |   .                       .                    .      ...............................     .
 |   .         Kafka         .                    .      .                             .     .
 |   .                       .                    .      .          Dataflow           .     .
 |   .  ...................  .     |-------------------> .                             .     .
 |   .  .                 .  .     |              .      .  ...........   ..........   .     .
 ------>.      Topic      .--------               .      .  .         .   .        .   .     .
     .  .                 .  .                    .      .  .  Worker .   . Worker .   .     .
     .  ...................  .            ---------------.  .         .   .        .   .     .
     .                       .           |        .      .  ...........   ..........   .     .
     .                       .           |        .      ...............................     .
     .  ...................  .           |        .                                          .
     .  .                 .<-------------|        .                                          .
     .  .      Topic      .  .                    .      ...............................     .
     .  .                 .--------|              .      .                             .     .
     .  ...................  .     |              .      .           Dataflow          .     .    ......................
     .                       .     |-------------------> .                             .     .    .                    .
     .                       .                    .      .  ..........    ..........   .     .    .                    .
     .  ...................  .                    .      .  .        .    .        .   .--------->.        Sink        .
     .  .                 .<---------------------------- .  . Worker .    . Worker .   .     .    .                    .
     .  .    Topic        .  .                    .      .  .        .    .        .   .     .    ......................
     .  .                 .---------|             .      .  ..........    ..........   .     .
     .  ...................  .      |             .      .                             .     .
     .                       .      |             .      ...............................     .
     .                       .      |             .                                          .
     .                       .      |             .                                          .
     .                       .      |             .      ...............................     .
     .........................      |             .      .                             .     .
                                    |             .      .          Dataflow           .     .
                                    |------------------> .                             .     .     .....................
                                                  .      .  ..........    ...........  .     .     .                   .
                                                  .      .  .        .    .         .  .     .     .                   .
                                                  .      .  . Worker .    .  Worker .  .---------->.       Sink        .
                                                  .      .  .        .    .         .  .     .     .                   .
                                                  .      .  ..........    ...........  .     .     ..................... 
                                                  .                                          .
                                                  ............................................

1. The design is after storm and ESB concept design.

2. The scribengin consist of 3 main components: Source, DataFlow and Sink.

 * Source is the data repository where the record should be read from and process. In order to archieve the parallel processing, the source should be splitable. Each chunk of source is named SourceStream. Each SourceStream implementation should support the transaction so it can remember the position of the last read of a reader. 
   - Kafka topic source is splitable since the data are stored on the multiple partitions. 
   - HDFS file source are also splitable, but we have to make sure that the file is locked during the read. We can also use zookeeper to implement lock and transaction for HDFS file reader.

 * Sink is the data repository where the processed, transformed record should be stored. In order to archieve the parallel processing, the sink should allow the concurrent write. 
   - Most of the data server such Kafka, Elasticsearch, HBase, Cassandra, they are supported the concurrent write, we just need to create a client to send the data to the server. 
   - The hdfs sink should allow multiple tmp segment for each SinkStream. When a SinkStream commit, we can move the tmp segment to another tmp dir. There should be another master process that suppose to manage and merge the commit segment in the commit tmp dir.

 * A Dataflow is a logic of a computation such as functions, filters, streaming joins, streaming aggregations... 
   - The dataflow consists of a master process, can be a yarn app master, and several workers. Each worker, can be the yarn allocated container, can have several executors. The master may use zookeeper to store the data and coordinate with the other dataflow process.
   - The dataflow master process take in the Source parameters, Sink parameters and the other parameters.
   - The dataflow master compute and split the source according to the parameters and the source structure into several tasks. Each task should have at least one SourceStream and one SinkStream, a task may have several source stream or sink stream in case it does the split or join. The tasks configuration or description should be stored in the zookeeper
   - The dataflow master request the yarn container or create the worker, pass the configuration to the worker and assign the task to the worker. Since the tasks are stored on zookeeper, the master can also pass the configuration location to the worker and the worker can pick up the task from zookeeper. We can have 2 ways to coordinate the master , workers. One is to use RPC, the master should use rpc to call the worker to get the report and status, commit the status to zookeeper. The other way is to use the zookeeper watch/notify feature. In this case, the worker is responsible to update the status itself, zookeeper will notify the master about the status update of the worker or if there is a worker die.

 * Storm topology can be archieved by defining a Dataflow chain that include multiple dataflow into one package and deploy to yarn

3. Tool can be developed to manage and monitor the dataflow or multiple dataflow by accessing the zookeeper to retrieve the the configuration, status of each data flow. It can also modify a dataflow configuration such rebalance the number of the workers or tasks. In this case the master should watch the configuration on the zookeeper and address to the change by killing or allocate more containers for the worker. 
