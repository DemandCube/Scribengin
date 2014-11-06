package com.neverwinterdp.scribengin.sink;

public interface Sink {
  public String getName();

  public DataStream[] getDataStreams();

  public void delete(DataStream stream) throws Exception;

  public DataStream newDataStream() throws Exception ;

  public void close() throws Exception ;
}
