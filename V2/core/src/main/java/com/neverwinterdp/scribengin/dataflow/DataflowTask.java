package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor.Status;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class DataflowTask {
  private DataflowContainer container;
  private DataflowTaskDescriptor descriptor;
  private ScribeAbstract processor;
  private DataflowTaskContext context;
  private boolean interrupt = false;
  private boolean complete = false;
  
  public DataflowTask(DataflowContainer container, DataflowTaskDescriptor descriptor) throws Exception {
    this.container = container;
    this.descriptor = descriptor;
    Class<ScribeAbstract> processorType = (Class<ScribeAbstract>) Class.forName(descriptor.getDataProcessor());
    processor = processorType.newInstance();
  }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public boolean isComplete() { return this.complete ; }
  
  public void interrupt() { interrupt = true; }
  
  public void init() throws Exception {
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    DataflowTaskReport report = dRegistry.getTaskReport(descriptor);
    if(report.getStartTime() == 0) {
      report.setStartTime(System.currentTimeMillis());
    }
    context = new DataflowTaskContext(container, descriptor, report);
    descriptor.setStatus(Status.PROCESSING);
    dRegistry.dataflowTaskUpdate(descriptor);
    dRegistry.dataflowTaskReport(descriptor, report);
  }
  
  public void run() throws Exception {
    DataflowTaskReport report = context.getReport();
    SourceStreamReader reader = context.getSourceStreamReader() ;
    Record record = null ;
    while(!interrupt && (record = reader.next()) != null) {
      processor.process(record, context);
      report.incrProcessCount();
    }
    if(!interrupt) complete = true;
  }
  
  public void suspend() throws Exception {
    saveContext();
    container.getDataflowRegistry().dataflowTaskSuspend(descriptor);
  }
  
  public void finish() throws Exception {
    saveContext();
    container.getDataflowRegistry().dataflowTaskFinish(descriptor);
  }
  
  void saveContext() throws Exception {
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    DataflowTaskReport report = context.getReport();
    context.commit();
    context.close();
    report.setFinishTime(System.currentTimeMillis());
    dRegistry.dataflowTaskReport(descriptor, report);
  }
}