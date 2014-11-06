package com.neverwinterdp.scribengin.dataflow;

public interface DataFlowTaskExecutor {
  static enum Status { INIT, RUNNING, TERMINATED }
  
  public String getServer() ;
  public int    getProcessId() ;
  public Status getStatus() ;
  public void   kill() ;
  
  public DataFlowTask       getDataFlowTask() ;
  public DataFlowTaskReport getDataFlowTaskReport() ;
}
