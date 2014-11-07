package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.dataflow.config.DataFlowConfig;

public interface DataFlowMaster {
  public DataFlowConfig getDataflowConfig() ;
  public DataFlowTask[] getDataFlowTask() ;
  public DataFlowWorker[] getDataflowWorker() ;
  
  public void onModify(DataFlowConfig config) ;
}
