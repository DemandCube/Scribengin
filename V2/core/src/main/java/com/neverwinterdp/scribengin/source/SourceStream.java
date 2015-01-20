package com.neverwinterdp.scribengin.source;

public interface SourceStream {
  public SourceStreamDescriptor getDescriptor() ;
  public SourceStreamReader     getReader(String name) throws Exception ;
}
