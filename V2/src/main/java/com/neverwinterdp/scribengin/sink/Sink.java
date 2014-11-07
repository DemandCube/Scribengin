package com.neverwinterdp.scribengin.sink;

public interface Sink {
  public String getName();

  public SinkStream[] getDataStreams();

  public void delete(SinkStream stream) throws Exception;

  public SinkStream newSinkStream() throws Exception ;

  public void close() throws Exception ;
}
