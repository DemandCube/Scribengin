package com.neverwinterdp.scribengin.dataflow;

public interface DataFlow {
  public String getName();

  public DataStream[] getDataStreams();

  public void delete(DataStream stream) throws Exception;

  public DataStream newDataStream();

  public DataFlowReader getReader();

  public void close();
}
