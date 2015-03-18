package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;

/**
 * @author Tuan Nguyen
 */
public interface Source {
  public StorageDescriptor getDescriptor() ;
  public SourceStream   getStream(int id) ;
  public SourceStream   getStream(StreamDescriptor descriptor) ;
  
  public SourceStream[] getStreams() ;
  
  public void close() throws Exception ;
}