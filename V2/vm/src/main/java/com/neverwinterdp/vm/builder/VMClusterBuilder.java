package com.neverwinterdp.vm.builder;

import java.util.List;

import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.event.VMAssertEventListener;

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
    VMAssertEventListener vmAssert = createVMMaster("vm-master-1");
    vmAssert.waitForEvents(60000);
  }
  
  public void shutdown() throws Exception {
    List<VMDescriptor> list = vmClient.getRunningVMDescriptors() ;
    for(VMDescriptor vmDescriptor : list) {
      vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    }
  }
  
  public VMAssertEventListener createVMMaster(String name) throws Exception {
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    VMAssertEventListener vmAssert = new VMAssertEventListener(vmClient.getRegistry());
    vmAssert.assertVMStatus(format("Expect %s with running status", name), name, VMStatus.RUNNING);
    vmAssert.assertHeartbeat(format("Expect %s has connected heartbeat", name), name, true);
    h1(format("Create VM master %s", name));
    vmClient.createVMMaster(name);
    return vmAssert;
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
  
  static public String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }
}
