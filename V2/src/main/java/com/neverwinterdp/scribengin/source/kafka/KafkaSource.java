package com.neverwinterdp.scribengin.source.kafka;

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

public class KafkaSource implements Source {
  //TODO: implement kafka source and assign each kafka partition as a source stream
  //TODO KafkaSourceDescriptor  for topic metadata?
  //Source descriptor defines topic?
  private SourceDescriptor descriptor;
  private Set<SourceStream> sourceStreams;


  public KafkaSource(SourceDescriptor descriptor) {
    super();
    this.descriptor = descriptor;
  }

  @Override
  public SourceDescriptor getSourceConfig() {
    return descriptor;
  }

  @Override
  public SourceStream getSourceStream(int id) {
    // TODO id is partition
    // TODO how ensure sourceStreams is populated
    return Iterables.getOnlyElement(Collections2.filter(sourceStreams, new IdPredicate(id)));
  }

  @Override
  public SourceStream getSourceStream(SourceStreamDescriptor descriptor) {
    // TODO how ensure sourceStreams is populated
    return Iterables.getOnlyElement(Collections2.filter(sourceStreams, new DescriptorPredicate(
        descriptor)));
  }

  @Override
  public SourceStream[] getSourceStreams() {
    //Create SourceStreams here
    /* TODO For each partition get a sourceStream
    REad zk, get partitions for topic
     create sourceStreams for each partiotions
     SourceStream should have 
    1. partition id
    2.  topic name
    3. Leader HostPort
    
    SourceStream will figure out start offset for topic/partition
    Monitor zk topic path for new partitions?
    */
    return  sourceStreams.toArray(new SourceStream[sourceStreams.size()]);
  }
}


class IdPredicate implements Predicate<SourceStream> {

  private int id;

  public IdPredicate(int id) {
    this.id = id;
  }

  @Override
  public boolean apply(SourceStream input) {
    return input.getDescriptor().getId() == id;
  }
}


class DescriptorPredicate implements Predicate<SourceStream> {

  private SourceStreamDescriptor descriptor;

  public DescriptorPredicate(SourceStreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public boolean apply(SourceStream input) {
    return input.getDescriptor().equals(descriptor);
  }
}
