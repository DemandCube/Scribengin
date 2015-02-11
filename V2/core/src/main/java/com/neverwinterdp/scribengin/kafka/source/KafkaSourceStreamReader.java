package com.neverwinterdp.scribengin.kafka.source;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.PartitionMetadata;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class KafkaSourceStreamReader implements SourceStreamReader {
  private SourceStreamDescriptor descriptor;
  private KafkaPartitionReader partitionReader ;
  
  public KafkaSourceStreamReader(SourceStreamDescriptor descriptor, PartitionMetadata partitionMetadata) {
    this.descriptor = descriptor;
    this.partitionReader = 
        new KafkaPartitionReader(descriptor.attribute("name"), descriptor.attribute("topic"), partitionMetadata);
  }
  
  @Override
  public String getName() { return descriptor.attribute("name"); }

  @Override
  public Record next() throws Exception {
    return partitionReader.nextAs(Record.class);
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

  static boolean printCommit = false;
  @Override
  public CommitPoint commit() throws Exception {
    partitionReader.commit();
    return null;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public boolean prepareCommit() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void completeCommit() {
    // TODO Auto-generated method stub
    
  }

}
