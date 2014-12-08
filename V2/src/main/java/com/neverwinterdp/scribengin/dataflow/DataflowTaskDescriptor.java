package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

public class DataflowTaskDescriptor {
  static public enum Status { INIT, PROCESSING, SUSPENDED, TERMINATED }

  private int                               id;
  private Status                            status = Status.INIT;
  private String                            dataProcessor;
  private SourceStreamDescriptor            sourceStreamDescriptor;
  private Map<String, SinkStreamDescriptor> sinkStreamDescriptors;

  public int getId() { return id; }
  public void setId(int id) { this.id = id; }

  public Status getStatus() { return this.status; }
  public void setStatus(Status status) { this.status = status; }
  
  public String getDataProcessor() { return dataProcessor;}
  public void setDataProcessor(String dataflowProcessor) {
    this.dataProcessor = dataflowProcessor;
  }
  
  public void setDataProcessor(Class<? extends DataProcessor> type) {
    this.dataProcessor = type.getName();
  }
  
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
  
  
}
