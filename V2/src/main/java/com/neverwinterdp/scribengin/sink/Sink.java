package com.neverwinterdp.scribengin.sink;

public interface Sink {
  public SinkDescriptor getDescriptor();
  
  public SinkStream[] getStreams();

  public void delete(SinkStream stream) throws Exception;

  public SinkStream newStream() throws Exception ;

  public void close() throws Exception ;
}
