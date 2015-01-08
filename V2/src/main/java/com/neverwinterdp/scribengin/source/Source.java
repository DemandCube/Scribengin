package com.neverwinterdp.scribengin.source;
/**
 * @author Tuan Nguyen
 */
public interface Source {
  public SourceDescriptor getDescriptor() ;
  public SourceStream   getStream(int id) ;
  public SourceStream   getStream(SourceStreamDescriptor descriptor) ;
  
  public SourceStream[] getStreams() ;
  
  public void close() throws Exception ;
}