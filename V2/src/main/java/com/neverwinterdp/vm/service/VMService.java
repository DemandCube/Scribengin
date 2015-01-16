package com.neverwinterdp.vm.service;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMRegistryListener;
import com.neverwinterdp.vm.VMStatus;

@Singleton
public class VMService {
  final static public String ALLOCATED_PATH = "/vm/allocated";
  final static public String HISTORY_PATH   = "/vm/history";
  final static public String LEADER_PATH    = "/vm/master/leader";
  
  private Registry registry;
  private VMRegistryListener vmListener ;
  
  @Inject
  private VMServicePlugin plugin ;
  
  @Inject
  public void init(Registry registry) throws Exception {
    this.registry = registry;
    registry.createIfNotExist(LEADER_PATH) ;
    registry.createIfNotExist(ALLOCATED_PATH) ;
    registry.createIfNotExist(HISTORY_PATH) ;
    vmListener = new VMRegistryListener(registry);
    vmListener.add(new HeartBeatManagementListener());
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }
  
  public Registry getRegistry() { return this.registry; }
  
  public VMRegistryListener getVMListenerManager() { return this.vmListener ; }
  
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
    vmListener.watch(descriptor);
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

  public class HeartBeatManagementListener implements VMRegistryListener.HeartBeatListener {
    @Override
    public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) {
    }

    @Override
    public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
      try {
        unregister(vmDescriptor);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
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