package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.scribe.partitioner.SinkPartitioner;

public interface Sink {
  public String getName();

  public SinkStream[] getDataStreams();
  
  public void setPartitioner(SinkPartitioner s);

  public void delete(SinkStream stream) throws Exception;

  public SinkStream newSinkStream() throws Exception ;

  public void close() throws Exception ;
}
