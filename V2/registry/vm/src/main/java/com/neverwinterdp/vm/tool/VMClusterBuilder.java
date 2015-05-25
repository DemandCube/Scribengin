package com.neverwinterdp.vm.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.service.VMService;

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
    WaitingNodeEventListener waitingListener = createVMMaster("vm-master-1");
    waitingListener.waitForEvents(30000);
    TabularFormater info = waitingListener.getTabularFormaterEventLogInfo();
    info.setTitle("Waiting for vm-master events to make sure it is launched properly");
    System.out.println(info.getFormatText()); 
  }
  
  public void shutdown() throws Exception {
    List<VMDescriptor> list = vmClient.getActiveVMDescriptors() ;
    for(VMDescriptor vmDescriptor : list) {
      vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    }
  }
  
  public WaitingNodeEventListener createVMMaster(String vmId) throws Exception {
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    
    WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(vmClient.getRegistry()) ;
    String vmStatusPath = VMService.getVMStatusPath(vmId);
    waitingListener.add(vmStatusPath, VMStatus.RUNNING, "Wait for RUNNING status for vm " + vmId, true);
    String vmHeartbeatPath = VMService.getVMHeartbeatPath(vmId);
    waitingListener.addCreate(vmHeartbeatPath, format("Expect %s has connected heartbeat", vmId), true);
    String vmServiceStatusPath = VMService.MASTER_PATH + "/status";
    waitingListener.add(vmServiceStatusPath, VMService.Status.RUNNING, "Wait for VMService RUNNING status ", true);
    h1(format("Create VM master %s", vmId));
    vmClient.createVMMaster(vmId);
    return waitingListener;
  }
  
  public <T> Injector newAppContainer() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    
    props.put("implementation:" + Registry.class.getName(), RegistryImpl.class.getName()) ;
    AppModule module = new AppModule(props) ;
    return Guice.createInjector(module);
  }
  
  public RegistryConfig getRegistryConfig() { return vmClient.getRegistry().getRegistryConfig() ; }
  
  public Registry newRegistry() {
    return new RegistryImpl(getRegistryConfig());
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
