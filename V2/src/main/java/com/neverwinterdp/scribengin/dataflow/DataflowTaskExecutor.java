package com.neverwinterdp.scribengin.dataflow;

public interface DataflowTaskExecutor {

  public DataflowTaskExecutorDescriptor getDescriptor() ;
  
  public void   kill() ;
  public DataflowTask       getDataflowTask() ;
  public DataflowTaskReport getDataflowTaskReport() ;
}
