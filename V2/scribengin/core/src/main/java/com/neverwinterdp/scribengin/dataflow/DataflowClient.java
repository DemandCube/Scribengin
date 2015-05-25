package com.neverwinterdp.scribengin.dataflow;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowClient {
  private ScribenginClient scribenginClient ;
  private DataflowRegistry dflRegistry ;
  
  public DataflowClient(ScribenginClient scribenginClient, String dataflowPath) throws Exception {
    this.scribenginClient = scribenginClient;
    dflRegistry = new DataflowRegistry(scribenginClient.getRegistry(), dataflowPath) ;
  }
  
  public Registry getRegistry() { return scribenginClient.getRegistry(); }
  
  public DataflowRegistry getDataflowRegistry() { return this.dflRegistry ; }
  
  public ScribenginClient getScribenginClient() { return this.scribenginClient; }
  
  
  public VMDescriptor getDataflowMaster() throws RegistryException { 
    return dflRegistry.getDataflowMaster() ;
  }
  
  public List<VMDescriptor> getDataflowMasters() throws RegistryException {
    return dflRegistry.getDataflowMasters();
  }
  
  public List<VMDescriptor> getActiveDataflowWorkers() throws RegistryException {
    return dflRegistry.getActiveWorkers();
  }
  
  public int countActiveDataflowWorkers() throws RegistryException {
    return dflRegistry.countActiveDataflowWorkers();
  }
  
  public void setDataflowEvent(DataflowEvent event) throws RegistryException {
    dflRegistry.broadcastMasterEvent(event);
  }
  
  public DataflowLifecycleStatus getStatus() throws RegistryException {
    return dflRegistry.getStatus() ;
  }
  
  public void waitForDataflowStatus(long timeout, DataflowLifecycleStatus status) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    while(System.currentTimeMillis() < stopTime) {
      if(status == dflRegistry.getStatus()) return;
      Thread.sleep(500);
    }
    throw new Exception("Cannot get the " + status + " after " + timeout + "ms");
  }
}