package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.stream.Stream;

public interface Dataflow {
  public DataflowConfig getDataflowConfig() ;
  public Stream[] getStreams() ;
  public DataflowWorker[] getDataflowWorker() ;

  public void onModify(DataflowConfig config) ;
  
  public String getName();
  public boolean killFlow();
  public boolean stopFlow();
  public boolean startFlow();
}

