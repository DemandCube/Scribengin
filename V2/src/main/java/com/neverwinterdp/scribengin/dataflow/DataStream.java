package com.neverwinterdp.scribengin.dataflow;

public interface DataStream {
  public int getIndex() ;
  public DataStreamReader getReader() ;
  public DataStreamWriter getWriter() ;
}
