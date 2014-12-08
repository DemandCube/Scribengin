package com.neverwinterdp.scribengin.dataflow;

public class DataflowWorkerDescriptor {
  private String hostname;
  private int    processId ;
  
  public String getHostname() { return hostname; }
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
  
  public int getProcessId() { return processId; }
  public void setProcessId(int processId) {
    this.processId = processId;
  }
}
