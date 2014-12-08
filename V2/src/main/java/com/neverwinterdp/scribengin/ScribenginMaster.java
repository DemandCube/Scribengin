package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowConfig;

@Singleton
public class ScribenginMaster {
  final static public String SCRIBENGIN_PATH = "/scribengin";
  final static public String LEADER_PATH     = "/scribengin/leader";
  final static public String DATAFLOWS_PATH  = "/scribengin/dataflows";
  
  private Registry registry;
  
  @Inject
  public void onInit(Registry registry) throws Exception {
    this.registry = registry;
    registry.createIfNotExist(DATAFLOWS_PATH);
  }
  
  public void onDestroy() throws Exception {
  }
  
  public void deploy(DataflowConfig config) throws Exception {
    
  }
}
