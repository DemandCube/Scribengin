package com.neverwinterdp.scribengin.dataflow;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.Registry;


public class DataflowMaster {
  @Inject @Named("dataflow.registry.path")
  private String dataflowRegistryPath;
  
  @Inject
  private Registry registry;
  
  @Inject
  public void onInit() throws Exception {
    System.out.println("onInit()");
    System.out.println("  dataflow.registry.path = " + registry);
    System.out.println("  registry               = " + dataflowRegistryPath);
  }
  
  public void onDestroy() throws Exception {
  }
}
