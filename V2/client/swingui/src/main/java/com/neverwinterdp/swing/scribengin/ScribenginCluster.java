package com.neverwinterdp.swing.scribengin;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.vm.client.VMClient;

abstract public class ScribenginCluster {
  static private ScribenginCluster CURRENT_INSTANCE ;
  
  public void start() throws Exception {
  }
  
  public void shutdown() throws Exception {
  }
  
  public void startDependencySevers() throws Exception {
  }
  
  public void shutdownDependencySevers() throws Exception {
  }
  
  abstract public void startVMMaster() throws Exception ;
  abstract public void shutdownVMMaster() throws Exception ;
  
  abstract public void startScribenginMaster() throws Exception ;
  abstract public void shutdownScribenginMaster() throws Exception ;
  
  
  abstract public ScribenginShell getScribenginShell() ;
  
  abstract public VMClient getVMClient() ;
  
  abstract public Registry getRegistry() ;

  static public ScribenginCluster getCurrentInstance() { return CURRENT_INSTANCE;  }
  
  static public void setCurrentInstance(ScribenginCluster instance) { 
    CURRENT_INSTANCE = instance ; 
  }
}
