package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.source.SourceStream;

public interface DataflowTaskContext {
  public SourceStream  getSourceStream();
  public SinkStream[]  getSinkStreams() ;
  
  public void write(Record record) throws Exception ;
  public void write(String sinkName, Record record) throws Exception ;
  public void commit() throws Exception;
  public void rollback() throws Exception;
}
