package com.neverwinterdp.vm.service;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.RegistryListener;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.event.VMHeartbeatNodeWatcher;

@Singleton
public class VMService {
  final static public String ALLOCATED_PATH = "/vm/allocated";
  final static public String HISTORY_PATH   = "/vm/history";
  final static public String LEADER_PATH    = "/vm/master/leader";
  final static public String EVENTS_PATH    = "/vm/events";
  
  @Inject
  private Registry registry;
  @Inject
  private RegistryListener registryListener;
  
  @Inject
  private VMServicePlugin plugin ;
  
  @Inject
  public void onInit() throws Exception {
    registry.createIfNotExist(LEADER_PATH) ;
    registry.createIfNotExist(EVENTS_PATH) ;
    registry.createIfNotExist(ALLOCATED_PATH) ;
    registry.createIfNotExist(HISTORY_PATH) ;
    
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }
  
  public Registry getRegistry() { return this.registry; }
  
  public VMDescriptor[] getAllocatedVMDescriptors() throws RegistryException {
    List<String> names = registry.getChildren(ALLOCATED_PATH) ;
    VMDescriptor[] descriptor = new VMDescriptor[names.size()];
    for(int i = 0; i < names.size(); i++) {
      String name = names.get(i) ;
      descriptor[i] = registry.getDataAs(ALLOCATED_PATH + "/" + name, VMDescriptor.class) ;
    }
    return descriptor;
  }
  
  public void register(VMDescriptor descriptor) throws Exception {
    Node vmNode = registry.create(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName(), NodeCreateMode.PERSISTENT);
    descriptor.setStoredPath(vmNode.getPath());
    vmNode.setData(descriptor);
    vmNode.createChild("status", VMStatus.ALLOCATED, NodeCreateMode.PERSISTENT);
    vmNode.createChild("commands", NodeCreateMode.PERSISTENT);
    watch(descriptor);
  }
  
  public void watch(VMDescriptor vmDescriptor) throws Exception {
    VMHeartbeatNodeWatcher heartbeatWatcher = new VMHeartbeatNodeWatcher(registry) {
      @Override
      public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
        try {
          unregister(vmDescriptor);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    String heartbeatPath = getVMHeartbeatPath(vmDescriptor.getVmConfig().getName());
    registryListener.watch(heartbeatPath, heartbeatWatcher, true);
  }
  
  public void unregister(VMDescriptor descriptor) throws Exception {
    //TODO: fix this check by removing the watcher
    if(isClosed()) return;
    if(!registry.exists(descriptor.getStoredPath())) return;
    //Copy the vm descriptor to the history path. This is not efficient, 
    //but zookeeper does not provide the move method
    Node vmNode = 
        registry.create(HISTORY_PATH + "/" + descriptor.getVmConfig().getName() + "-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    vmNode.setData(descriptor);
    
    //Recursively delete the vm data in the allocated path
    registry.rdelete(descriptor.getStoredPath());
  }
  
  public boolean isRunning(VMDescriptor descriptor) throws Exception {
    Node statusNode = registry.get(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName() + "/status");
    if(!statusNode.exists()) return false;
    VMStatus status = statusNode.getData(VMStatus.class);
    if(status == VMStatus.ALLOCATED) {
      return true;
    } else {
      return statusNode.hasChild("heartbeat");
    }
  }
  
  public void kill(VMDescriptor descriptor) throws Exception {
    plugin.killVM(this, descriptor);
  }

  public VMDescriptor allocate(VMConfig vmConfig) throws RegistryException, Exception {
    VMDescriptor vmDescriptor = new VMDescriptor(vmConfig);
    register(vmDescriptor);
    plugin.allocateVM(this, vmConfig);
    return vmDescriptor;
  }
  
  static public String getVMStatusPath(String vmName) { return ALLOCATED_PATH + "/" + vmName + "/status" ; }
  
  static public String getVMHeartbeatPath(String vmName) { return ALLOCATED_PATH + "/" + vmName + "/status/heartbeat" ; }
  
  static public void register(Registry registry, VMDescriptor descriptor) throws Exception {
    registry.createIfNotExist(ALLOCATED_PATH) ;
    registry.createIfNotExist(LEADER_PATH) ;
    Node vmNode = registry.create(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName(), NodeCreateMode.PERSISTENT);
    descriptor.setStoredPath(vmNode.getPath());
    vmNode.setData(descriptor);
    vmNode.createChild("status", VMStatus.ALLOCATED, NodeCreateMode.PERSISTENT);
    vmNode.createChild("commands", NodeCreateMode.PERSISTENT);
  }
}