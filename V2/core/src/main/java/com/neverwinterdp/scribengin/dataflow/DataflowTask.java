package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.SourceStreamReader;


public class DataflowTask {
  private DataflowTaskDescriptor descriptor;
  private DataProcessor processor;
  private DataflowTaskContext context;
  
  public DataflowTask(DataflowContainer container, DataflowTaskDescriptor descriptor) throws Exception {
    this.descriptor = descriptor;
    Class<DataProcessor> processorType = 
        (Class<DataProcessor>) Class.forName(descriptor.getDataProcessor());
    processor = processorType.newInstance();
    context = new DataflowTaskContext(container, descriptor);
  }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
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