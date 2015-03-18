package com.neverwinterdp.scribengin.kafka.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;

public class KafkaSource implements Source {
  private StorageDescriptor descriptor;
  private Map<Integer, KafkaSourceStream> sourceStreams = new HashMap<Integer, KafkaSourceStream>();
  
  public KafkaSource(String name, String zkConnect, String topic) throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    init(descriptor);
  }
  
  public KafkaSource(StorageDescriptor descriptor) throws Exception {
    init(descriptor);
  }

  void init(StorageDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    KafkaTool kafkaTool = new KafkaTool(descriptor.attribute("name"), descriptor.attribute("zk.connect"));
    kafkaTool.connect();
    TopicMetadata topicMetdadata = kafkaTool.findTopicMetadata(descriptor.attribute("topic"));
    List<PartitionMetadata> partitionMetadatas = topicMetdadata.partitionsMetadata();
    for(int i = 0; i < partitionMetadatas.size(); i++) {
      PartitionMetadata partitionMetadata = partitionMetadatas.get(i);
      KafkaSourceStream sourceStream = new KafkaSourceStream(descriptor, partitionMetadata);
      sourceStreams.put(sourceStream.getId(), sourceStream);
    }
    kafkaTool.close();
  }
  
  @Override
  public StorageDescriptor getDescriptor() { return descriptor; }

  /**
   * The stream id is equivalent to the partition id of the kafka
   */
  @Override
  public SourceStream getStream(int id) {  return sourceStreams.get(id); }

  @Override
  public SourceStream getStream(StreamDescriptor descriptor) {
    return sourceStreams.get(descriptor.getId());
  }

  @Override
  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[sourceStreams.size()];
    return sourceStreams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
}