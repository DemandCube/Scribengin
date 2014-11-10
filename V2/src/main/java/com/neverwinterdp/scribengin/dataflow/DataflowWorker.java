package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.stream.StreamExecutor;

public interface DataflowWorker {
  public DataflowWorkerDescriptor getDescriptor() ;
  public StreamExecutor[] getExecutors() ;
}
