package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

public class DataflowTaskDescriptor {
  static public enum Status { INIT, PROCESSING, SUSPENDED, TERMINATED }

  private int                               id;
  private Status                            status = Status.INIT;
  private String                            scribe;
  private SourceStreamDescriptor            sourceStreamDescriptor;
  private Map<String, SinkStreamDescriptor> sinkStreamDescriptors;
  private String                            storedPath;
  
  public int getId() { return id; }
  public void setId(int id) { this.id = id; }

  public Status getStatus() { return this.status; }
  public void setStatus(Status status) { this.status = status; }
  
  public String getScribe() { return scribe;}
  public void setScribe(String scribe) { this.scribe = scribe; }
  
  public SourceStreamDescriptor getSourceStreamDescriptor() { return sourceStreamDescriptor; }
  public void setSourceStreamDescriptor(SourceStreamDescriptor sourceStreamDescriptor) {
    this.sourceStreamDescriptor = sourceStreamDescriptor;
  }
  
  public void add(String name, SinkStreamDescriptor sinkDescriptor) {
    if(sinkStreamDescriptors == null) {
      sinkStreamDescriptors = new HashMap<String, SinkStreamDescriptor>() ;
    }
    sinkStreamDescriptors.put(name, sinkDescriptor);
  }
  
  public Map<String, SinkStreamDescriptor> getSinkStreamDescriptors() { return sinkStreamDescriptors; }
  public void setSinkStreamDescriptors(Map<String, SinkStreamDescriptor> sinkStreamDescriptors) {
    this.sinkStreamDescriptors = sinkStreamDescriptors;
  }
  
  @JsonIgnore
  public String getStoredPath() { return storedPath; }
  public void setStoredPath(String storedPath) { this.storedPath = storedPath; }
  
  public String storedName() {
    if(this.storedPath == null) {
      throw new RuntimeException("Stored path is not available") ;
    }
    int idx = storedPath.lastIndexOf("/") ;
    String storedName = storedPath.substring(idx + 1);
    return storedName;
  }
}
