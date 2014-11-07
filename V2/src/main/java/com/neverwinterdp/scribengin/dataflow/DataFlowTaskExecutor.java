package com.neverwinterdp.scribengin.dataflow;

public interface DataFlowTaskExecutor {

  public DataFlowTaskExecutorDescriptor getDescriptor() ;
  
  public void   kill() ;
  public DataflowTask       getDataflowTask() ;
  public DataFlowTaskReport getDataflowTaskReport() ;
}
