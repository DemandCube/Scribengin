package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class DataflowDescriptor {
  private String                      name;
  private String                      dataflowAppHome;
  private SourceDescriptor            sourceDescriptor;
  private Map<String, SinkDescriptor> sinkDescriptors;
  private int                         numberOfWorkers =  1;
  private int                         numberOfExecutorsPerWorker = 1;
  private long                        taskMaxExecuteTime = -1;
  
  private String                      scribe;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getDataflowAppHome() {
    return dataflowAppHome;
  }
  public void setDataflowAppHome(String dataflowAppHome) {
    this.dataflowAppHome = dataflowAppHome;
  }

  public SourceDescriptor getSourceDescriptor() { return sourceDescriptor;}
  public void setSourceDescriptor(SourceDescriptor sourceDescriptor) { this.sourceDescriptor = sourceDescriptor;}

  public void addSinkDescriptor(String name, SinkDescriptor descriptor) {
    if(sinkDescriptors == null) sinkDescriptors = new HashMap<String, SinkDescriptor>();
    sinkDescriptors.put(name, descriptor);
  }
  
  public Map<String, SinkDescriptor> getSinkDescriptors() { return sinkDescriptors; }
  public void setSinkDescriptors(Map<String, SinkDescriptor> sinkDescriptors) {
    this.sinkDescriptors = sinkDescriptors;
  }
  
  public int getNumberOfWorkers() { return numberOfWorkers; }
  public void setNumberOfWorkers(int numberOfWorkers) { this.numberOfWorkers = numberOfWorkers; }
  
  public int getNumberOfExecutorsPerWorker() { return numberOfExecutorsPerWorker; }
  public void setNumberOfExecutorsPerWorker(int number) {
    this.numberOfExecutorsPerWorker = number;
  }
  
  public long getTaskMaxExecuteTime() { return taskMaxExecuteTime;}
  public void setTaskMaxExecuteTime(long taskMaxExecuteTime) {
    this.taskMaxExecuteTime = taskMaxExecuteTime;
  }
  
  public String getScribe() { return scribe; }
  public void setScribe(String scribe) { this.scribe = scribe; }
}