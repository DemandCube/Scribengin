package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;

public class DataflowDescriptor {
  private String                         id  ;
  private String                         name;
  private String                         dataflowAppHome;
  private StorageDescriptor              storageDescriptor;
  private Map<String, StorageDescriptor> sinkDescriptors;
  private int                            numberOfWorkers            =  1;
  private int                            numberOfExecutorsPerWorker =  1;
  private long                           taskMaxExecuteTime         = -1;
  private String                         scribe;

  public String getId() { return id; }
  public void setId(String id)  { this.id = id; }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String getDataflowAppHome() { return dataflowAppHome; }
  public void setDataflowAppHome(String dataflowAppHome) { this.dataflowAppHome = dataflowAppHome;  }

  public StorageDescriptor getSourceDescriptor() { return storageDescriptor;}
  public void setSourceDescriptor(StorageDescriptor storageDescriptor) { this.storageDescriptor = storageDescriptor;}

  public void addSinkDescriptor(String name, StorageDescriptor descriptor) {
    if(sinkDescriptors == null) sinkDescriptors = new HashMap<String, StorageDescriptor>();
    sinkDescriptors.put(name, descriptor);
  }
  
  public Map<String, StorageDescriptor> getSinkDescriptors() { return sinkDescriptors; }
  public void setSinkDescriptors(Map<String, StorageDescriptor> sinkDescriptors) {
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