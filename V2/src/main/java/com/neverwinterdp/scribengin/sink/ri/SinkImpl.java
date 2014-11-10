package com.neverwinterdp.scribengin.sink.ri;

import java.util.LinkedHashMap;

import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;

public class SinkImpl implements Sink {
  private String name ;
  private int indexTracker = 0;
  
  private LinkedHashMap<Integer, SinkStream> datastreams = new LinkedHashMap<Integer, SinkStream>() ;
  
  public SinkImpl(String name) {
    this.name = name ;
  }
  
  public String getName() { return this.name ; }

  synchronized public SinkStream[] getDataStreams() {
    SinkStream[] array = new SinkStream[datastreams.size()] ;
    datastreams.values().toArray(array) ;
    return array;
  }

  @Override
  synchronized public void delete(SinkStream stream) throws Exception {
    SinkStream foundStream = datastreams.remove(stream.getId()) ;
    if(foundStream == null) {
      throw new Exception("Cannot find the stream " + stream.getId()) ;
    }
  }
  
  @Override
  synchronized public SinkStream newSinkStream() {
    SinkStreamImpl datastream = new SinkStreamImpl(indexTracker++);
    datastreams.put(datastream.getId(), datastream) ;
    return datastream;
  }

  @Override
  public void close() throws Exception  { 
    
  }
}
