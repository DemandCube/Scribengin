package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamReader;


public class DataflowTask {
  private DataflowTaskDescriptor descriptor;
  private DataProcessor processor;
  private DataflowTaskContext context;
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public void onInit(DataflowTaskDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    Class<DataProcessor> processorType = 
        (Class<DataProcessor>)Class.forName(descriptor.getDataProcessor());
    processor = processorType.newInstance();
    context = new DataflowTaskContext(descriptor);
  }
  
  public void onDestroy() throws Exception {
  }

  public void execute() throws Exception {
    SourceStreamReader reader = context.getSourceStreamReader() ;
    Record record = null ;
    while((record = reader.next()) != null) {
      processor.process(record, context);
    }
    context.close();
  }
  
  public void suspend() throws Exception {
    
  }
}