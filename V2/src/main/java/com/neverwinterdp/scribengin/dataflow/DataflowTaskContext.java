package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.source.SourceStream;

public class DataflowTaskContext {
  private SourceStream            sourceStream;
  private Map<String, SinkStream> sinkStreams = new HashMap<String, SinkStream>();
  
  public SourceStream  getSourceStream() { return sourceStream; }
  
  public SinkStream[]  getSinkStreams() {
    return null ;
  }
  
  public void write(Record record) throws Exception {
    
  }
  
  public void write(String sinkName, Record record) throws Exception {
    
  }
  
  public void commit() throws Exception {
    
  }
  
  public void rollback() throws Exception {
    
  }
}
