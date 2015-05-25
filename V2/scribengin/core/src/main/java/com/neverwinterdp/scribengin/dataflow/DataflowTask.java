package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class DataflowTask {
  private DataflowContainer container;
  private final TaskContext<DataflowTaskDescriptor> taskContext;
  private DataflowTaskDescriptor descriptor ;
  private ScribeAbstract processor;
  private DataflowTaskContext context;
  private boolean interrupt = false;
  private boolean complete = false;
  
  public DataflowTask(DataflowContainer container, TaskContext<DataflowTaskDescriptor> taskContext) throws Exception {
    this.container = container;
    this.taskContext = taskContext;
    this.descriptor = taskContext.getTaskDescriptor(true);
    Class<ScribeAbstract> scribeType = (Class<ScribeAbstract>) Class.forName(descriptor.getScribe());
    processor = scribeType.newInstance();
  }
  
  public TaskContext<DataflowTaskDescriptor> getTaskContext() { return this.taskContext; }
  
  public DataflowTaskDescriptor getDescriptor() { return descriptor ; }
  
  public boolean isComplete() { return this.complete ; }
  
  public void interrupt() { interrupt = true; }
  
  public void init() throws Exception {
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    DataflowTaskReport report = dRegistry.getTaskReport(descriptor);
    context = new DataflowTaskContext(container, descriptor, report);
  }
  
  public void run() throws Exception {
    DataflowTaskReport report = context.getReport();
    SourceStreamReader reader = context.getSourceStreamReader() ;
    Record record = null ;
    while(!interrupt && (record = reader.next()) != null) {
      report.incrProcessCount();
      processor.process(record, context);
    }
    if(!interrupt) complete = true;
  }
  
  public void suspend() throws Exception {
    saveContext();
    container.getDataflowRegistry().dataflowTaskSuspend(taskContext);
  }
  
  public void finish() throws Exception {
    saveContext();
    container.getDataflowRegistry().dataflowTaskFinish(taskContext);
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