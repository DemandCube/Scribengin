package com.neverwinterdp.scribengin.dataflow.ri;

import java.util.LinkedHashMap;

import com.neverwinterdp.scribengin.dataflow.DataFlow;
import com.neverwinterdp.scribengin.dataflow.DataFlowReader;
import com.neverwinterdp.scribengin.dataflow.DataStream;

public class DataFlowImpl implements DataFlow {
  private String name ;
  private int indexTracker = 0;
  private LinkedHashMap<Integer, DataStream> datastreams = new LinkedHashMap<Integer, DataStream>() ;
  
  public DataFlowImpl(String name) {
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
    datastreams.remove(stream.getIndex()) ;
  }

  @Override
  synchronized public DataStream newDataStream() {
    DataStreamImpl datastream = new DataStreamImpl(indexTracker++);
    datastreams.put(datastream.getIndex(), datastream) ;
    return datastream;
  }

  @Override
  public DataFlowReader getReader() {
    return new DataFlowReaderImpl(getDataStreams());
  }

  @Override
  public void close() { }
}
