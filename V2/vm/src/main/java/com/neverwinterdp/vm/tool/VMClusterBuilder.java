package com.neverwinterdp.vm.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.event.VMWaitingEventListener;
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
    VMWaitingEventListener waitingListener = createVMMaster("vm-master-1");
    waitingListener.waitForEvents(60000);
    TabularFormater info = waitingListener.getTabularFormaterEventLogInfo();
    info.setTitle("Waiting for vm-master events to make sure it is launched properly");
    System.out.println(info.getFormatText()); 
  }
  
  public void shutdown() throws Exception {
    List<VMDescriptor> list = vmClient.getRunningVMDescriptors() ;
    for(VMDescriptor vmDescriptor : list) {
      vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    }
  }
  
  public VMWaitingEventListener createVMMaster(String name) throws Exception {
    if(!vmClient.getRegistry().isConnect()) {
      vmClient.getRegistry().connect() ;
    }
    VMWaitingEventListener waitingListener = new VMWaitingEventListener(vmClient.getRegistry());
    waitingListener.waitVMStatus(format("Expect %s with running status", name), name, VMStatus.RUNNING);
    waitingListener.waitHeartbeat(format("Expect %s has connected heartbeat", name), name, true);
    waitingListener.waitVMServiceStatus("Wait for the master status running", VMService.Status.RUNNING);
    h1(format("Create VM master %s", name));
    vmClient.createVMMaster(name);
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
