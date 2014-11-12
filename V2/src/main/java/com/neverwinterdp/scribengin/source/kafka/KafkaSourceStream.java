package com.neverwinterdp.scribengin.source.kafka;

import java.util.Set;

import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

/**
 * The kafka source reads from zk how many partitions the topic has and starts sourceStreams for each partition
 * 
 * SourceStream has reader to read
 * */
public class KafkaSourceStream implements SourceStream {

  private SourceStreamDescriptor sourceStreamDescriptor;
  private SourceStreamReader sourceStreamReader;
  private HostPort leader;
  private Set<HostPort>  brokers;
  
  public KafkaSourceStream(SourceStreamDescriptor sourceStreamDescriptor, SourceStreamReader sourceStreamReader) {
    this.sourceStreamDescriptor = sourceStreamDescriptor;
    this.sourceStreamReader= sourceStreamReader;
  }

  @Override
  public SourceStreamDescriptor getDescriptor() {
    return sourceStreamDescriptor;
  }

  @Override
  public SourceStreamReader getReader(String name) {
    return sourceStreamReader;
  }

  public HostPort getLeader() {
      return leader;
  }
}
