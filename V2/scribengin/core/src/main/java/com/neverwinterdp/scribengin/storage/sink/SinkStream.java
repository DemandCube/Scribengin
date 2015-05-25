package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;

public interface SinkStream {
  public StreamDescriptor getDescriptor();
  public void delete() throws Exception;
  public SinkStreamWriter getWriter() throws Exception ;
}
