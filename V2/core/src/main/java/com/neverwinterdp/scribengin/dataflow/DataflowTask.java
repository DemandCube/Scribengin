package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor.Status;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

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
    Class<ScribeAbstract> scribeType = (Class<ScribeAbstract>) Class.forName(descriptor.getScribe());
    processor = scribeType.newInstance();
  }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public boolean isComplete() { return this.complete ; }
  
  public void interrupt() { interrupt = true; }
  
  public void init() throws Exception {
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    DataflowTaskReport report = dRegistry.getTaskReport(descriptor);
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
      report.incrProcessCount();
      processor.process(record, context);
    }
    if(!interrupt) {
      complete = true;
    }
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
    context.commit();
    context.close();
    DataflowTaskReport report = context.getReport();
    report.setFinishTime(System.currentTimeMillis());
    dRegistry.dataflowTaskReport(descriptor, report);
  }
}