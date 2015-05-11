package com.neverwinterdp.scribengin.builder;

import static com.neverwinterdp.vm.tool.VMClusterBuilder.h1;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class ScribenginClusterBuilder {
  private VMClusterBuilder vmClusterBuilder ;
  private ScribenginClient scribenginClient;
  
  public ScribenginClusterBuilder(VMClusterBuilder vmClusterBuilder) {
    this.vmClusterBuilder = vmClusterBuilder ;
  }
  
  public VMClusterBuilder getVMClusterBuilder() { return vmClusterBuilder ; }
  
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
    ScribenginWaitingEventListener sribenginWaitingEvents = 
        new ScribenginWaitingEventListener(vmClusterBuilder.getVMClient().getRegistry());
    h1("Create Scribengin Master 1");
    sribenginWaitingEvents.waitScribenginMaster("Expect vm-scribengin-master-1 as the leader", "vm-scribengin-master-1");
    scribenginClient.createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    sribenginWaitingEvents.waitForEvents(60000);
    TabularFormater info = sribenginWaitingEvents.getTabularFormaterEventLogInfo();
    info.setTitle("Wait for scribengin master 1 events to make sure it is launched properly");
    System.out.println(info.getFormatText());
    
    h1("Create Scribengin Master 2");
    sribenginWaitingEvents = 
        new ScribenginWaitingEventListener(vmClusterBuilder.getVMClient().getRegistry());
    sribenginWaitingEvents.waitVMStatus("vm-scribengin-master-2 running", "vm-scribengin-master-2", VMStatus.RUNNING);
    scribenginClient.createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    sribenginWaitingEvents.waitForEvents(60000);
    info = sribenginWaitingEvents.getTabularFormaterEventLogInfo();
    info.setTitle("Wait for scribengin master 2 events to make sure it is launched properly");
    System.out.println(info.getFormatText());
  }

  public void shutdown() throws Exception {
    scribenginClient.shutdown();
    Thread.sleep(2000);
    vmClusterBuilder.shutdown();
  }
}
