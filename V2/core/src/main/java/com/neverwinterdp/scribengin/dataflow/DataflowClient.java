package com.neverwinterdp.scribengin.dataflow;

import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.util.DataflowTaskNodeDebugger;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowClient {
  private ScribenginClient scribenginClient ;
  private DataflowRegistry dflRegistry ;
  
  public DataflowClient(ScribenginClient scribenginClient, String dataflowPath) throws Exception {
    this.scribenginClient = scribenginClient;
    dflRegistry = new DataflowRegistry(scribenginClient.getRegistry(), dataflowPath) ;
  }
  
  public ScribenginClient getScribenginClient() { return this.scribenginClient; }
  
  
  public VMDescriptor getDataflowMaster() throws RegistryException { 
    return dflRegistry.getDataflowMaster() ;
  }
  
  public List<VMDescriptor> getDataflowMasters() throws RegistryException {
    return dflRegistry.getDataflowMasters();
  }
  
  public List<VMDescriptor> getDataflowWorkers() throws RegistryException {
    return dflRegistry.getActiveWorkers();
  }
  
  public void setDataflowTaskEvent(DataflowTaskEvent event) throws RegistryException {
    dflRegistry.setDataflowTaskMasterEvent(event);
  }
  
  public RegistryDebugger getDataflowTaskDebugger(Appendable out) throws RegistryException {
    RegistryDebugger debugger = new RegistryDebugger(out, scribenginClient.getVMClient().getRegistry()) ;
    debugger.watchChild("/scribengin/dataflows/running/hello-kafka-dataflow/tasks/executors/assigned", ".*", new DataflowTaskNodeDebugger());
    //return debugger ;
    return dflRegistry.getDataflowTaskDebugger(out) ;
  }
}