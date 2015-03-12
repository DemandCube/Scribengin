package com.neverwinterdp.scribengin.s3.sink;

import java.io.IOException;
import java.util.LinkedHashMap;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class S3Sink implements Sink {
  private SinkDescriptor descriptor;

  private int idTracker = 0;
  private LinkedHashMap<Integer, S3SinkStream> streams = new LinkedHashMap<Integer, S3SinkStream>() ;
  private Injector injector;
  
  @Inject
  public S3Sink(Injector injector,  SinkDescriptor descriptor) {
    this.injector = injector;
    this.descriptor = descriptor;
    
  }
  
  public SinkDescriptor getDescriptor() { return this.descriptor; }
  
  public SinkStream  getStream(SinkStreamDescriptor descriptor) throws Exception {
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
    SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor("S3", id, location) ;
    streamDescriptor.putAll(descriptor);
    S3SinkStream stream = new S3SinkStream(injector, streamDescriptor);
    streams.put(streamDescriptor.getId(), stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
  

}