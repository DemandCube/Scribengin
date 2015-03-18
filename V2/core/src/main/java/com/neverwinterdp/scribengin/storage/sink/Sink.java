package com.neverwinterdp.scribengin.storage.sink;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;

public interface Sink {
  public StorageDescriptor getDescriptor();
  
  public SinkStream  getStream(StreamDescriptor descriptor) throws Exception ;
  
  public SinkStream[] getStreams();

  public void delete(SinkStream stream) throws Exception;

  public SinkStream newStream() throws Exception ;

  public void close() throws Exception ;
}
