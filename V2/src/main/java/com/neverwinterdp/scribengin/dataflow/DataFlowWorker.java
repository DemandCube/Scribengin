package com.neverwinterdp.scribengin.dataflow;

public interface DataFlowWorker {
  public DataFlowWorkerDescriptor getDescriptor() ;
  public DataFlowTaskExecutor[] getExecutors() ;
}
