package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;

public interface DataflowMaster {
  public DataflowConfig getDataflowConfig() ;
  public DataflowTask[] getDataflowTask() ;
  public DataflowWorker[] getDataflowWorker() ;

  public void onModify(DataflowConfig config) ;
}

