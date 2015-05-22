package com.neverwinterdp.swing.tool;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.vm.client.VMClient;

abstract public class Cluster {
  static private Cluster CURRENT_INSTANCE ;
  
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

  static public Cluster getCurrentInstance() { return CURRENT_INSTANCE;  }
  
  static public void setCurrentInstance(Cluster instance) { 
    CURRENT_INSTANCE = instance ; 
  }
}
