package com.neverwinterdp.scribengin.dataflow;

import com.google.inject.Inject;
import com.neverwinterdp.registry.Registry;


public class DataflowMaster {
  final static public String SCRIBENGIN_DATAFLOWS = "/scribengin/dataflows" ;
  
  private Registry registry;
  
  @Inject
  public void onInit(Registry registry) throws Exception {
    this.registry = registry;
    registry.createIfNotExist(SCRIBENGIN_DATAFLOWS);
  }
  
  public void onDestroy() throws Exception {
  }
  
  public void deploy(DataflowConfig config) throws Exception {
    
  }
}
