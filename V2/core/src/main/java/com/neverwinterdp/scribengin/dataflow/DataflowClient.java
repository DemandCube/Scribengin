package com.neverwinterdp.scribengin.dataflow;

import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowClient {
  private ScribenginClient scribenginClient ;
  private DataflowRegistry dataflowRegistry ;
  
  public DataflowClient(ScribenginClient scribenginClient, String dataflowPath) throws Exception {
    this.scribenginClient = scribenginClient;
    this.dataflowRegistry = new DataflowRegistry(scribenginClient.getRegistry(), dataflowPath) ;
  }
  
  public ScribenginClient getScribenginClient() { return this.scribenginClient; }
  
  
  public VMDescriptor getDataflowMaster() throws RegistryException { 
    return dataflowRegistry.getDataflowMaster() ;
  }
  
  public List<VMDescriptor> getDataflowMasters() throws RegistryException {
    return dataflowRegistry.getDataflowMasters();
  }
  
  public List<VMDescriptor> getDataflowWorkers() throws RegistryException {
    return dataflowRegistry.getWorkers();
  }
}