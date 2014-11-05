package com.neverwinterdp.scribengin.sink.ri;

import java.util.LinkedHashMap;

import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.DataStream;

public class SinkImpl implements Sink {
  private String name ;
  private int indexTracker = 0;
  
  private LinkedHashMap<Integer, DataStream> datastreams = new LinkedHashMap<Integer, DataStream>() ;
  
  public SinkImpl(String name) {
    this.name = name ;
  }
  
  public String getName() { return this.name ; }

  synchronized public DataStream[] getDataStreams() {
    DataStream[] array = new DataStream[datastreams.size()] ;
    datastreams.values().toArray(array) ;
    return array;
  }

  @Override
  synchronized public void delete(DataStream stream) throws Exception {
    DataStream foundStream = datastreams.remove(stream.getIndex()) ;
    if(foundStream == null) {
      throw new Exception("Cannot find the stream " + stream.getIndex()) ;
    }
  }
  
  @Override
  synchronized public DataStream newDataStream() {
    DataStreamImpl datastream = new DataStreamImpl(indexTracker++);
    datastreams.put(datastream.getIndex(), datastream) ;
    return datastream;
  }

  @Override
  public void close() throws Exception  { 
    
  }
}
