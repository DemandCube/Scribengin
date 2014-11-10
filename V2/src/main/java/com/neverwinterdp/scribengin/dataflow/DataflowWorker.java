package com.neverwinterdp.scribengin.dataflow;

public interface DataflowWorker {
  public DataflowWorkerDescriptor getDescriptor() ;
  public DataflowTaskExecutor[] getExecutors() ;
}
