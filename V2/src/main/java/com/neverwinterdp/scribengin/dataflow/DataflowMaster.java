package com.neverwinterdp.scribengin.dataflow;


public interface DataflowMaster {
  public DataflowConfig getDataflowConfig() ;
  public DataflowTask[] getDataflowTask() ;
  public DataflowWorker[] getDataflowWorker() ;

  public void onModify(DataflowConfig config) ;
}