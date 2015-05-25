package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;

public interface SourceStream {
  public StreamDescriptor getDescriptor() ;
  public SourceStreamReader     getReader(String name) throws Exception ;
}
