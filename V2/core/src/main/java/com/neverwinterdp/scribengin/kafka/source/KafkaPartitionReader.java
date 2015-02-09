package com.neverwinterdp.scribengin.kafka.source;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class KafkaPartitionReader {
  private String name;
  private String topic ;
  private PartitionMetadata partitionMetadata;
  private SimpleConsumer consumer;
  private long readOffset;

  public KafkaPartitionReader(String name, String topic, PartitionMetadata partitionMetadata) {
    this.name = name;
    this.topic = topic;
    this.partitionMetadata = partitionMetadata;
    Broker broker = partitionMetadata.leader();
    consumer = 
        new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, name);
    readOffset = getLastOffset(kafka.api.OffsetRequest.EarliestTime());
  }
  
  public List<byte[]> fetch(int fetchSize) throws Exception {
    FetchRequest req = 
        new FetchRequestBuilder().
        clientId(name).
        // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
        addFetch(topic, partitionMetadata.partitionId(), readOffset, fetchSize).
        minBytes(1).
        maxWait(1000).
        build();
    FetchResponse fetchResponse = consumer.fetch(req);

    if(fetchResponse.hasError()) {
      throw new Exception("TODO: handle the error, reset the consumer....");
    }
    List<byte[]> holder = new ArrayList<byte[]>();
    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionMetadata.partitionId())) {
      long currentOffset = messageAndOffset.offset();
      if (currentOffset < readOffset) {
        System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
        continue;
      }
      readOffset = messageAndOffset.nextOffset();
      ByteBuffer payload = messageAndOffset.message().payload();

      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      holder.add(bytes);
    }
    return holder ;
  }
  
  long getLastOffset(long whichTime) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionMetadata.partitionId());
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), name);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out.println(
        "Error fetching data Offset Data the Broker. Reason: " + 
        response.errorCode(topic, partitionMetadata.partitionId())
      );
      return 0;
    }
    long[] offsets = response.offsets(topic, partitionMetadata.partitionId());
    return offsets[0];
  }
}