#Design#

While being designed all documentation will be put in the google doc under implementation:
<https://docs.google.com/document/d/1Hr7lefftUP5Kn_uho9ITpWAd1wm_YCZsowHY4auEOfI/edit#>

I have updated and left the diagrams in the Readme.md so the spacing isn't disturbed. 

##Diagram##

```
                                               ............................................
                                               .                                          .
     .........................                 .                Zookeeper                 .
     .                       .                 .        Dataflow Worker Coordinator       .
     .        Source         .                 .                                          .
 - - . (HDFS, App, File...)  .                 ............................................
 |   .                       . 
 |   .........................                 ............................................
 |                                             .                                          .
 |                                             .                 Yarn                     .
 |   .........................                 .                                          .
 |   .                       .                 .      ...............................     .
 |   .         Kafka         .                 .      .                             .     .
 |   .                       .                 .      .          Dataflow           .     .
 |   .  ...................  .     |----------------> .         (df-name 1)         .     .
 |   .  .                 .  .     |           .      .  ...........   ..........   .     .
 ------>.      Topic      .--------            .      .  .         .   .        .   .     .
     .  .                 .  .                 .      .  .  Scribe .   . Scribe .   .     .
     .  ...................  .            ------------.  .         .   .        .   .     .
     .                       .           |     .      .  ...........   ..........   .     .
     .                       .           |     .      ...............................     .
     .  ...................  .           |     .                                          .
     .  .                 .<-------------|     .                                          .
     .  .      Topic      .  .                 .      ...............................     .
     .  .                 .--------|           .      .                             .     .
     .  ...................  .     |           .      .           Dataflow          .     .    ......................
     .                       .     |----------------> .          (df-name 2)        .     .    .                    .
     .                       .                 .      .  ..........    ..........   .     .    .                    .
     .  ...................  .                 .      .  .        .    .        .   .--------->.     Destination    .
     .  .                 .  .                 .      .  . Scribe .    . Scribe .   .     .    .        System      .
     .  .    Topic        .  .                 .      .  .        .    .        .   .     .    ......................
     .  .                 .---------|          .      .  ..........    ..........   .     .
     .  ...................  .      |          .      .                             .     .
     .                       .      |          .      ...............................     .
     .                       .      |          .                                          .
     .                       .      |          .                                          .
     .                       .      |          .      ...............................     .
     .........................      |          .      .                             .     .
                                    |          .      .          Dataflow           .     .
                                    |---------------> .         (df-name 3)         .     .     .....................
                                               .      .  ..........    ...........  .     .     .                   .
                                               .      .  .        .    .         .  .     .     .                   .
                                               .      .  . Scribe .    .  Scribe .  .---------->.    Destination    .
                                               .      .  .        .    .         .  .     .     .       System      .
                                               .      .  ..........    ...........  .     .     ..................... 
                                               .                                          .
                                               ............................................

```


##Code Design##

```

##Scribengin Data structure##
  ScribenginRegistry
    locks/
      sources/
        source-descriptor-1
          stream-descriptor-1
            lock
            commit-log
          stream-descriptor-2
        source-descriptor-2
          stream-descriptor-1
          stream-descriptor-2
      sinks
        sink-descriptor-1
          stream-descriptor-1
          stream-descriptor-2
        sink-descriptor-2
          stream-descriptor-1
          stream-descriptor-2
    dataflows
      dataflow1
        config
        task-descriptors
          task-descriptor-1
            sources
              source-descriptor-1
                stream-descriptor-1
            sinks
              sink-descriptor-1
                stream-descriptor-1
          task-descriptor-2
          task-descriptor-3
          ....
        workers
          worker-descriptor-1
            task-executor-descriptor-1
              task-descriptor-ref-1
            task-executor-descriptor-2
              task-descriptor-ref-2
          worker-descriptor-2

```
