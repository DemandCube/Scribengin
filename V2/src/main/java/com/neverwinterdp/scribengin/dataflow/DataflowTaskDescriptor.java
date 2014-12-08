package com.neverwinterdp.scribengin.dataflow;

import java.util.Map;

import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

public class DataflowTaskDescriptor {
  static public enum Status {
    INIT, PROCESSING, SUSPENDED, TERMINATED
  }

  private int                               id;
  private Status                            status = Status.INIT;
  private SourceStreamDescriptor            sourceStreamDescriptor;
  private Map<String, SinkStreamDescriptor> sinkStreamDescriptors;

  public int getId() { return id; }
  public void setId(int id) { this.id = id; }

  public Status getStatus() { return this.status; }
  public void setStatus(Status status) { this.status = status; }
}
