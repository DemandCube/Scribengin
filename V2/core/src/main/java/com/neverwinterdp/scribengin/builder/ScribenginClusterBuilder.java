package com.neverwinterdp.scribengin.builder;

import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.scribengin.junit.ScribenginAssert;
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
  
  public ScribenginClient getScribenginClient() { return this.scribenginClient ; }
  
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
    scribenginClient = new ScribenginClient(vmClient.getRegistry());
    ScribenginAssert sribenginAssert = new ScribenginAssert(vmClusterBuilder.getVMClient().getRegistry());
    h1("Create Scribengin Master 1 and 2");
    sribenginAssert.assertScribenginMaster("Expect vm-scribengin-master-1 as the leader", "vm-scribengin-master-1");
    scribenginClient.createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    scribenginClient.createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    sribenginAssert.waitForEvents(30000);
  }

  public void shutdown() throws Exception {
    vmClusterBuilder.shutdown();
  }
}
