package com.neverwinterdp.scribengin.builder;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

import static com.neverwinterdp.vm.builder.VMClusterBuilder.*;

import com.neverwinterdp.vm.client.VMClient;

public class ScribenginClusterBuilder {
  private VMClusterBuilder vmClusterBuilder ;
  private ScribenginClient scribenginClient;
  
  public ScribenginClusterBuilder(VMClusterBuilder vmClusterBuilder) {
    this.vmClusterBuilder = vmClusterBuilder ;
  }
  
  public VMClusterBuilder getVMClusterBuilder() { return this.vmClusterBuilder ; }
  
  public ScribenginClient getScribenginClient() { 
    if(scribenginClient == null) {
      scribenginClient = new ScribenginClient(vmClusterBuilder.getVMClient());
    }
    return this.scribenginClient ; 
  }
  
  public void clean() throws Exception {
    vmClusterBuilder.clean(); 
  }
  
  public void start() throws Exception {
    startVMMasters() ;
    startScribenginMasters();
  }
  
  public void startVMMasters() throws Exception {
    vmClusterBuilder.start(); 
  }
  
  public void startScribenginMasters() throws Exception {
    VMClient vmClient = vmClusterBuilder.getVMClient();
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    scribenginClient = new ScribenginClient(vmClient);
    ScribenginWaitingEventListener sribenginAssert = new ScribenginWaitingEventListener(vmClusterBuilder.getVMClient().getRegistry());
    h1("Create Scribengin Master 1");
    sribenginAssert.waitScribenginMaster("Expect vm-scribengin-master-1 as the leader", "vm-scribengin-master-1");
    scribenginClient.createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    sribenginAssert.waitForEvents(60000);
    h2("Finish creating Scribengin Master 1") ;
    
    h1("Create Scribengin Master 2");
    sribenginAssert.waitVMStatus("vm-scribengin-master-2 running", "vm-scribengin-master-2", VMStatus.RUNNING);
    scribenginClient.createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    sribenginAssert.waitForEvents(60000);
    h2("Finish creating Scribengin Master 2") ;
  }

  public void shutdown() throws Exception {
    vmClusterBuilder.shutdown();
  }
}
