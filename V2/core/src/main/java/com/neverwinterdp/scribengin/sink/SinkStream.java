package com.neverwinterdp.scribengin.sink;

public interface SinkStream {
  public SinkStreamDescriptor getDescriptor();
  public void delete() throws Exception;
  public SinkStreamWriter getWriter() throws Exception ;
}
