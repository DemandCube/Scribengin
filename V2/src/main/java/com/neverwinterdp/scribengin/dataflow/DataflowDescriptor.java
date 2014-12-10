package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class DataflowDescriptor {
  private String                      name;
  private SourceDescriptor            sourceDescriptor;
  private Map<String, SinkDescriptor> sinkDescriptors;
  private int                         numberOfWorkers;
  private int                         numberOfTasks;
  private String                      dataProcessor;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
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
  
  public int getNumberOfTasks() { return numberOfTasks; }
  public void setNumberOfTasks(int numberOfTasks) {
    this.numberOfTasks = numberOfTasks;
  }
  
  public String getDataProcessor() { return dataProcessor; }
  public void setDataProcessor(String dataProcessor) {
    this.dataProcessor = dataProcessor;
  }
}