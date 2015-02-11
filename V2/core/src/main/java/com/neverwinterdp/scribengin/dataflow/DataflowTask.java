package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.scribe.ScribeInterface;
import com.neverwinterdp.scribengin.source.SourceStreamReader;


public class DataflowTask {
  private DataflowContainer container;
  private DataflowTaskDescriptor descriptor;
  private ScribeInterface processor;
  private DataflowTaskContext context;
  
  public DataflowTask(DataflowContainer container, DataflowTaskDescriptor descriptor) throws Exception {
    this.container = container;
    this.descriptor = descriptor;
    Class<ScribeInterface> processorType = (Class<ScribeInterface>) Class.forName(descriptor.getDataProcessor());
    processor = processorType.newInstance();
    DataflowTaskReport report = new DataflowTaskReport();
    context = new DataflowTaskContext(container, descriptor, report);
  }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  
  public void execute() throws Exception {
    DataflowTaskReporter reporter = container.getDataflowTaskReporter();
    DataflowTaskReport report = context.getReport();
    report.setStartTime(System.currentTimeMillis());
    reporter.create(container, descriptor,  context.getReport());
    SourceStreamReader reader = context.getSourceStreamReader() ;
    Record record = null ;
    while((record = reader.next()) != null) {
      processor.process(record, context);
      report.incrProcessCount();
    }
    context.commit();
    context.close();
    report.setFinishTime(System.currentTimeMillis());
    reporter.report(container, descriptor,  context.getReport());
  }
  
  public void suspend() throws Exception {
  }
}