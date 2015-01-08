package com.neverwinterdp.scribengin.kafka.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.scribengin.kafka.KafkaClient;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

public class SourceImpl implements Source {
  private SourceDescriptor descriptor;
  private KafkaClient kafkaClient ;
  private Map<Integer, SourceStreamImpl> sourceStreams = new HashMap<Integer, SourceStreamImpl>();
  
  public SourceImpl(String name, String zkConnect, String topic) throws Exception {
    SourceDescriptor descriptor = new SourceDescriptor("kafka");
    descriptor.attribute("name", name);
    descriptor.attribute("topic", topic);
    descriptor.attribute("zk.connect", zkConnect);
    init(descriptor);
  }
  
  public SourceImpl(SourceDescriptor descriptor) throws Exception {
    init(descriptor);
  }

  void init(SourceDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    kafkaClient = new KafkaClient(descriptor.attribute("name"), descriptor.attribute("zk.connect"));
    kafkaClient.connect();
    TopicMetadata topicMetdadata = kafkaClient.findTopicMetadata(descriptor.attribute("topic"));
    List<PartitionMetadata> partitionMetadatas = topicMetdadata.partitionsMetadata();
    for(int i = 0; i < partitionMetadatas.size(); i++) {
      PartitionMetadata partitionMetadata = partitionMetadatas.get(i);
      SourceStreamImpl sourceStream = new SourceStreamImpl(descriptor, partitionMetadata);
      sourceStreams.put(sourceStream.getId(), sourceStream);
    }
  }
  
  @Override
  public SourceDescriptor getDescriptor() { return descriptor; }

  /**
   * The stream id is equivalent to the partition id of the kafka
   */
  @Override
  public SourceStream getStream(int id) {  return sourceStreams.get(id); }

  @Override
  public SourceStream getStream(SourceStreamDescriptor descriptor) {
    return sourceStreams.get(descriptor.getId());
  }

  @Override
  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[sourceStreams.size()];
    return sourceStreams.values().toArray(array);
  }
  
  public void close() throws Exception {
    kafkaClient.close();
  }
}