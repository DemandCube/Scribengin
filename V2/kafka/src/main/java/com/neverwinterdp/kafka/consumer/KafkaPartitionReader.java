package com.neverwinterdp.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.util.JSONSerializer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


public class KafkaPartitionReader {
  private String name;
  private String topic ;
  private PartitionMetadata partitionMetadata;
  private int fetchSize = 100000;
  private SimpleConsumer consumer;
  private long currentOffset;
  private ByteBufferMessageSet       currentMessageSet;
  private Iterator<MessageAndOffset> currentMessageSetIterator;

  public KafkaPartitionReader(String name, String topic, PartitionMetadata partitionMetadata) {
    this.name = name;
    this.topic = topic;
    this.partitionMetadata = partitionMetadata;
    Broker broker = partitionMetadata.leader();
    consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, name);
    currentOffset = getLastCommitOffset();
  }
  
  public void setFetchSize(int size) { this.fetchSize = size; }
  
  public void commit() throws Exception {
    saveOffsetInKafka(this.currentOffset, (short)0) ;
  }
  
  short saveOffsetInKafka(long offset, short errorCode) throws Exception{
    short versionID = 0;
    int correlationId = 0;
    TopicAndPartition tp = new TopicAndPartition(topic, partitionMetadata.partitionId());
    OffsetAndMetadata offsetAndMeta = new OffsetAndMetadata(offset, OffsetAndMetadata.NoMetadata(), errorCode);
    Map<TopicAndPartition, OffsetAndMetadata> mapForCommitOffset = new HashMap<TopicAndPartition, OffsetAndMetadata>();
    mapForCommitOffset.put(tp, offsetAndMeta);
    OffsetCommitRequest offsetCommitReq = new OffsetCommitRequest(name, mapForCommitOffset, correlationId, name, versionID);
    OffsetCommitResponse offsetCommitResp = consumer.commitOffsets(offsetCommitReq);
    return (Short) offsetCommitResp.errors().get(tp);
  }

  public void close() throws Exception {
    consumer.close();
  }
  
  public byte[] next() throws Exception {
    if(currentMessageSetIterator == null) nextMessageSet();
    byte[] payload = getCurrentMessagePayload();
    if(payload != null) return payload;
    nextMessageSet();
    return getCurrentMessagePayload();
  }
  
  public <T> T nextAs(Class<T> type) throws Exception {
    byte[] data = next();
    if(data == null) return null;
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }

  public List<byte[]> next(int maxRead) throws Exception {
    List<byte[]> holder = new ArrayList<>() ;
    for(int i = 0; i < maxRead; i++) {
      byte[] payload = next() ;
      if(payload == null) return holder;
      holder.add(payload);
    }
    return holder;
  }
  
  public List<byte[]> fetch(int fetchSize, int maxRead) throws Exception {
    return fetch(fetchSize, maxRead, 1000) ;
  }
  
  public List<byte[]> fetch(int fetchSize, int maxRead, int maxWait) throws Exception {
    FetchRequest req = 
        new FetchRequestBuilder().
        clientId(name).
        addFetch(topic, partitionMetadata.partitionId(), currentOffset, fetchSize).
        minBytes(1).
        maxWait(maxWait).
        build();
    
    FetchResponse fetchResponse = consumer.fetch(req);
    if(fetchResponse.hasError()) {
      throw new Exception("TODO: handle the error, reset the consumer....");
    }
    List<byte[]> holder = new ArrayList<byte[]>();
    ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, partitionMetadata.partitionId());
    int count = 0;
    for(MessageAndOffset messageAndOffset : messageSet) {
      if (messageAndOffset.offset() < currentOffset) continue; //old offset, ignore
      ByteBuffer payload = messageAndOffset.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      holder.add(bytes);
      currentOffset = messageAndOffset.nextOffset();
      count++;
      if(count == maxRead) break;
    }
    return holder ;
  }
  
  byte[] getCurrentMessagePayload() {
    while(currentMessageSetIterator.hasNext()) {
      MessageAndOffset messageAndOffset = currentMessageSetIterator.next();
      if (messageAndOffset.offset() < currentOffset) continue; //old offset, ignore
      ByteBuffer payload = messageAndOffset.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      currentOffset = messageAndOffset.nextOffset();
      return bytes;
    }
    return null;
  }
  
  void nextMessageSet() throws Exception {
    FetchRequest req = 
        new FetchRequestBuilder().
        clientId(name).
        addFetch(topic, partitionMetadata.partitionId(), currentOffset, fetchSize).
        minBytes(1).
        maxWait(1000).
        build();
    
    FetchResponse fetchResponse = consumer.fetch(req);
    if(fetchResponse.hasError()) {
      throw new Exception("TODO: handle the error, reset the consumer....");
    }
    
    currentMessageSet = fetchResponse.messageSet(topic, partitionMetadata.partitionId());
    currentMessageSetIterator = currentMessageSet.iterator();
  }
  
  long getLastCommitOffset() {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionMetadata.partitionId());
    List<TopicAndPartition> topicAndPartitions = new ArrayList<>();
    topicAndPartitions.add(topicAndPartition);
    OffsetFetchRequest oRequest = new OffsetFetchRequest(name, topicAndPartitions, (short) 0, 0, name);
    OffsetFetchResponse oResponse = consumer.fetchOffsets(oRequest);
    Map<TopicAndPartition, OffsetMetadataAndError> offsets = oResponse.offsets();
    OffsetMetadataAndError offset = offsets.get(topicAndPartition);
    long currOffset = offset.offset() ;
    if(currOffset < 0) currOffset = 0;
    return currOffset;
  }
}