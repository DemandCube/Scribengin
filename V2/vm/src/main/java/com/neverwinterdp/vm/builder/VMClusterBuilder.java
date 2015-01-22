package com.neverwinterdp.vm.builder;

import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.junit.VMAssert;

public class VMClusterBuilder {
  protected VMClient vmClient ;
  
  public VMClusterBuilder() {
  }
  
  public VMClusterBuilder(VMClient vmClient) {
    this.vmClient = vmClient ;
  }
  
  public VMClient getVMClient() { return this.vmClient ; }
  
  protected void init(VMClient vmClient) {
    this.vmClient = vmClient;
  }
  
  public void clean() throws Exception {
  }
  
  public void start() throws Exception {
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    VMAssert vmAssert = new VMAssert(vmClient.getRegistry());
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING);
    vmAssert.assertHeartbeat("Expect vm-master-1 has connected heartbeat", "vm-master-1", true);
    h1("Create VM master vm-master-1");
    vmClient.createVMMaster("vm-master-1");
    vmAssert.waitForEvents(30000);
  }
  
  public void shutdown() throws Exception {
  }
  
  
  static public void h1(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  static public void h2(String title) {
    System.out.println(title);
    StringBuilder b = new StringBuilder() ; 
    for(int i = 0; i < title.length(); i++) {
      b.append("-");
    }
    System.out.println(b) ;
  }
}
