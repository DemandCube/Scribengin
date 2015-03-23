package com.neverwinterdp.scribengin.storage.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class KafkaSourceStream implements SourceStream {
  private StreamDescriptor descriptor;
  private PartitionMetadata partitionMetadata;
  
  public KafkaSourceStream(StreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  public KafkaSourceStream(StorageDescriptor storageDescriptor, PartitionMetadata metadata) {
    descriptor = new StreamDescriptor(storageDescriptor);
    descriptor.setId(metadata.partitionId());
    this.partitionMetadata = metadata;
  }
  
  public int getId() { return descriptor.getId(); }
  
  @Override
  public StreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public SourceStreamReader getReader(String name) throws Exception {
    return new KafkaSourceStreamReader(descriptor, partitionMetadata);
  }
}