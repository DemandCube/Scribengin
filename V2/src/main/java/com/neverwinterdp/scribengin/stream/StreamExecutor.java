package com.neverwinterdp.scribengin.stream;

public interface StreamExecutor {

  public StreamExecutorDescriptor getDescriptor() ;
  
  public void   kill() ;
  public Stream       getDataflowTask() ;
  public StreamReport getDataflowTaskReport() ;
}
