package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamReader;


public class DataflowTask {
  private DataflowTaskDescriptor descriptor;
  private DataProcessor processor;
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public void onInit(DataflowTaskDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    this.processor = new CopyDataProcessor();
  }
  
  public void onDestroy() throws Exception {
    
  }
  
  public void execute() throws Exception {
    SourceStream sourceStream = null ;
    SourceStreamReader reader = sourceStream.getReader("reader") ;
    DataflowTaskContext ctx = null ;
    Record record = null ;
    while((record = reader.next()) != null) {
      processor.process(record, ctx);
    }
    ctx.commit();
  }
  
  public void suspend() throws Exception {
    
  }
}