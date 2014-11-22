package com.neverwinterdp.scribengin.dataflow.ri;

import com.neverwinterdp.scribengin.dataflow.DataflowConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowMaster;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowWorker;

public class DataflowMasterImpl implements DataflowMaster {

  @Override
  public DataflowConfig getDataflowConfig() {return null;
  }

  @Override
  public DataflowTask[] getDataflowTask() { return null; }

  @Override
  public DataflowWorker[] getDataflowWorker() { return null; }

  @Override
  public void onModify(DataflowConfig config) {
    
  }
  
  public void onInit() throws Exception {
    
  }
  
  public void start() throws Exception {
    
  }
  
  public void stop() throws Exception {
    
  }

  public void onDestroy() throws Exception {
    
  }
}
