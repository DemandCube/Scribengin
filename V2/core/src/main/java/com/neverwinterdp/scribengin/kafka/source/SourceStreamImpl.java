package com.neverwinterdp.scribengin.kafka.source;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class SourceStreamImpl implements SourceStream {
  private SourceStreamDescriptor descriptor;
  private PartitionMetadata partitionMetadata;
  
  public SourceStreamImpl(SourceStreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  public SourceStreamImpl(SourceDescriptor sourceDescriptor, PartitionMetadata metadata) {
    descriptor = new SourceStreamDescriptor(sourceDescriptor);
    descriptor.setId(metadata.partitionId());
    this.partitionMetadata = metadata;
  }
  
  public int getId() { return descriptor.getId(); }
  
  @Override
  public SourceStreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public SourceStreamReader getReader(String name) throws Exception {
    return new SourceStreamReaderImpl(descriptor, partitionMetadata);
  }
}