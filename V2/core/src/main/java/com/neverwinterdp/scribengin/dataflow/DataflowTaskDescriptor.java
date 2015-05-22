package com.neverwinterdp.scribengin.dataflow;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;

public class DataflowTaskDescriptor {
  final static public Comparator<DataflowTaskDescriptor> COMPARATOR = new Comparator<DataflowTaskDescriptor>() {
    @Override
    public int compare(DataflowTaskDescriptor o1, DataflowTaskDescriptor o2) {
      return o1.getTaskId().compareTo(o2.getTaskId());
    }
  };
  
  private String                        taskId;
  private String                        scribe;
  private StreamDescriptor              streamDescriptor;
  private Map<String, StreamDescriptor> sinkStreamDescriptors;
  private String                        registryPath;

  public String getTaskId() { return taskId; }
  public void setTaskId(String id) { this.taskId = id; }

  public String getScribe() { return scribe;}
  public void setScribe(String scribe) { this.scribe = scribe; }
  
  public StreamDescriptor getSourceStreamDescriptor() { return streamDescriptor; }
  public void setSourceStreamDescriptor(StreamDescriptor streamDescriptor) {
    this.streamDescriptor = streamDescriptor;
  }
  
  public void add(String name, StreamDescriptor sinkDescriptor) {
    if(sinkStreamDescriptors == null) {
      sinkStreamDescriptors = new HashMap<String, StreamDescriptor>() ;
    }
    sinkStreamDescriptors.put(name, sinkDescriptor);
  }
  
  public Map<String, StreamDescriptor> getSinkStreamDescriptors() { return sinkStreamDescriptors; }
  public void setSinkStreamDescriptors(Map<String, StreamDescriptor> sinkStreamDescriptors) {
    this.sinkStreamDescriptors = sinkStreamDescriptors;
  }
  
  @JsonIgnore
  public String getRegistryPath() { return registryPath; }
  public void   setRegistryPath(String path) { this.registryPath = path; }
  
  public String storedName() {
    if(this.registryPath == null) {
      throw new RuntimeException("Stored path is not available") ;
    }
    int idx = registryPath.lastIndexOf("/") ;
    String storedName = registryPath.substring(idx + 1);
    return storedName;
  }
}
