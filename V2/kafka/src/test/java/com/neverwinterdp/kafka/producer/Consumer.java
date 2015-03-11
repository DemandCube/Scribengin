package com.neverwinterdp.kafka.producer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
//TODO use pre-existing consumer KafkaMessageTool
public class Consumer implements AutoCloseable, Callable<List<String>> {

  private static final Logger logger = Logger.getLogger(Consumer.class);
  private static final int BUFFER_SIZE = 1024 * 1024;
  private static final int TIMEOUT = 99999;
  List<String> messages;
  private String topic;
  private int partition;
  private SimpleConsumer consumer;
  private boolean hasNextOffset;

  public Consumer(String leader, String topic2, int partition2) {
    this.topic = topic2;
    this.partition = partition2;
    messages = new LinkedList<String>();
    try {
      String host = leader.split(":")[0];
      int port = Integer.parseInt(leader.split(":")[1]);
      consumer = new SimpleConsumer(host, port, TIMEOUT, BUFFER_SIZE, "test-consumer");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public List<String> read() {
    long currentOffset = getOffset(kafka.api.OffsetRequest.EarliestTime());
    do {
      FetchRequest req = new FetchRequestBuilder().clientId("test Consumer")
          .addFetch(topic, partition, currentOffset, BUFFER_SIZE).build();

      FetchResponse resp = consumer.fetch(req);
      if (resp.hasError()) {
        logger.info("Error! " + resp.errorCode(topic, partition));
      }
      byte[] bytes = null;
      long nextOffset = currentOffset;
      for (MessageAndOffset messageAndOffset : resp.messageSet(topic, partition)) {
        long messageOffset = messageAndOffset.offset();
        if (messageOffset < currentOffset) {
          logger.info("Found an old offset: " + messageOffset + " Expecting: " + currentOffset);
          continue;
        }

        ByteBuffer payload = messageAndOffset.message().payload();
        bytes = new byte[payload.limit()];
        payload.get(bytes);
        messages.add(new String(bytes));
        logger.info("current offset " + currentOffset + " " + messageAndOffset.offset() + ": " + new String(bytes));
        nextOffset = messageAndOffset.nextOffset();
      }
      logger.info("currentOffset:" + currentOffset + " nextOffset:" + nextOffset);
      if (currentOffset < nextOffset) {
        hasNextOffset = true;
      } else {
        hasNextOffset = false;
      }
      currentOffset = nextOffset;
    } while (hasNextOffset);
    return messages;
  }

  /**
   * To get Earliest offset ask for kafka.api.OffsetRequest.EarliestTime(). To get latest offset ask
   * for kafka.api.OffsetRequest.LatestTime()
   */
  public long getOffset(long time) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    offsetInfo
        .put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(time, 1));
    logger.info("cunsumer is null " + consumer == null);
    OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest
        .CurrentVersion(), "test-consumer"));
    long[] endOffset = response.offsets(topic, partition);
    return endOffset[0];
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  public List<String> call() throws Exception {
    return read();
  }
}
