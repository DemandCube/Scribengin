package com.neverwinterdp.scribengin.source.kafka;

import java.nio.ByteBuffer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamReader;


/**
 * A reader of an individual partition in kafka. 
 * 
 * The partition is contained in sourceStream.descriptor, the topic is name
 * */
//Auditing? metrics?
public class KafkaSourceStreamReader implements SourceStreamReader {

  private String name;
  private KafkaSourceStream dataStream;
  private SimpleConsumer consumer;
  private CommitPoint commitPoint;
  public CommitPoint getCommitPoint() {
    return commitPoint;
  }

  public void setCommitPoint(CommitPoint commitPoint) {
    this.commitPoint = commitPoint;
  }


  private static final int BUFFER_SIZE = 64 * 1024;
  private static final int TIMEOUT = 10000;
  private static final Logger logger = Logger.getLogger(KafkaSourceStreamReader.class);

  public KafkaSourceStreamReader(String name, KafkaSourceStream sourceStream) {
    this.name = name;
    this.dataStream = sourceStream;
    init();
  }

  private void init() {
    consumer =
        new SimpleConsumer(dataStream.getLeader().getHost(), dataStream.getLeader()
            .getPort(), TIMEOUT, BUFFER_SIZE, getClientName());
  }


  @Override
  public String getName() {
    return name;
  }


  @Override
  public Record next() throws Exception {
    //ensure consumer is not null and not closed.
    //TODO read next offset, move commitpoint
    //TODO update commitpoint in registry?
    //TODO initialize a proper KafkaSourceStream
    //TODO handle exceptions
    String topic = name;
    int partition = dataStream.getDescriptor().getId();
    long endOffset = getCommitPointFromRepository().getEndOffset();

    FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, endOffset, 100000)
        .build();

    FetchResponse resp = consumer.fetch(req);

    Record record = null;

    for (MessageAndOffset messageAndOffset : resp.messageSet(topic, partition)) {
      long currentOffset = messageAndOffset.offset();
      endOffset = messageAndOffset.nextOffset();
      ByteBuffer payload = messageAndOffset.message().payload();

      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      record = new Record(topic, bytes);
      commitPoint = new CommitPoint(name, currentOffset, endOffset);


      logger.info("Concatenating for tmp string: " + String.valueOf(messageAndOffset.offset())
          + ": " + new String(bytes));
    }
    return record;
  }



  private CommitPoint getCommitPointFromRepository() {
    // TODO Read it from registry?
    return new CommitPoint();
  }


  @Override
  public Record[] next(int size) throws Exception {
    Record[] records = new Record[size];
    Record record;
    for (int i = 0; i < records.length; i++) {
      record = next();
      records[i] = record;
    }

    return records;
  }

  private String getClientName() {
    return "scribe_" + name + "_" + dataStream.getDescriptor().getId();
  }

  @Override
  public void rollback() throws Exception {
    // TODO commitpoint?
  }

  @Override
  public CommitPoint commit() throws Exception {
    // TODO write commit point to registry
    return null;
  }

  @Override
  public void close() throws Exception {
    consumer.close();
  }

  public void setDataStream(KafkaSourceStream dataStream) {
    this.dataStream = dataStream;
  }


  @Override
  public SourceStream getDataStream() {
    return dataStream;
  }
}
