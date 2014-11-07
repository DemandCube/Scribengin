package com.neverwinterdp.scribengin.dataflow;


public class DataFlowTaskExecutorDescriptor {
  static enum Status { INIT, RUNNING, TERMINATED }
  
  private Status status = Status.INIT ;
  
  public Status getStatus() { return status ; }
  
  public void setStatus(Status status) {
    this.status = status ;
  }
}
