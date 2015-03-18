package com.neverwinterdp.scribengin.nizarS3.sink;

import java.io.IOException;
import java.util.LinkedHashMap;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;

public class S3Sink implements Sink {
  private StorageDescriptor descriptor;

  private int idTracker = 0;
  private LinkedHashMap<Integer, S3SinkStream> streams = new LinkedHashMap<Integer, S3SinkStream>() ;
  private Injector injector;
  
  @Inject
  public S3Sink(Injector injector,  StorageDescriptor descriptor) {
    this.injector = injector;
    this.descriptor = descriptor;
    
  }
  
  public StorageDescriptor getDescriptor() { return this.descriptor; }
  
  public SinkStream  getStream(StreamDescriptor descriptor) throws Exception {
    SinkStream stream = streams.get(descriptor.getId());
    if(stream == null) {
      throw new Exception("Cannot find the stream " + descriptor.getId()) ;
    }
    return stream ;
  }
  
  synchronized public SinkStream[] getStreams() {
    SinkStream[] array = new SinkStream[streams.size()] ;
    streams.values().toArray(array) ;
    return array;
  }

  @Override
  synchronized public void delete(SinkStream stream) throws Exception {
    SinkStream foundStream = streams.remove(stream.getDescriptor().getId()) ;
    if(foundStream == null) {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getId()) ;
    }
  }
  
  @Override
  synchronized public SinkStream newStream() throws IOException {
    int id = idTracker++;
    String location = descriptor.getLocation() + "/stream-" + id;
    StreamDescriptor streamDescriptor = new StreamDescriptor("S3", id, location) ;
    streamDescriptor.putAll(descriptor);
    S3SinkStream stream = new S3SinkStream(injector, streamDescriptor);
    streams.put(streamDescriptor.getId(), stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
}