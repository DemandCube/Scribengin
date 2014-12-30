package com.neverwinterdp.scribengin.kafka.source;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class SourceStreamReaderImpl implements SourceStreamReader {
  private SourceStreamDescriptor descriptor;
  private KafkaPartitionReader partitionReader ;
  private KafkaPartitionReaderIterator partitionReaderIterator;
  
  public SourceStreamReaderImpl(SourceStreamDescriptor descriptor, PartitionMetadata partitionMetadata) {
    this.descriptor = descriptor;
    this.partitionReader = 
        new KafkaPartitionReader(descriptor.attribute("name"), descriptor.attribute("topic"), partitionMetadata);
    this.partitionReaderIterator = new KafkaPartitionReaderIterator(partitionReader);
  }
  
  @Override
  public String getName() { return descriptor.attribute("name"); }

  @Override
  public Record next() throws Exception {
    if(partitionReaderIterator.hasNext()) return partitionReaderIterator.nextAs(Record.class);
    return null;
  }

  @Override
  public Record[] next(int size) throws Exception {
    List<Record> holder = new ArrayList<Record>();
    Record[] array = new Record[holder.size()];
    return holder.toArray(array);
  }

  @Override
  public void rollback() throws Exception {
    throw new Exception("To implement") ;
  }

  @Override
  public CommitPoint commit() throws Exception {
    throw new Exception("To implement") ;
  }

  @Override
  public void close() throws Exception {
  }

}
