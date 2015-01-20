package com.neverwinterdp.vm;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

@Singleton
public class VMRegistry {
  @Inject @Named("vm.registry.allocated.path")
  private String vmAllocatedPath;
  
  @Inject
  private Registry registry ;

  public VMRegistry() {}
  
  public VMRegistry(Registry registry, String vmAllocatedPath) {
    this.registry = registry ;
    this.vmAllocatedPath = vmAllocatedPath;
  }
  
  public Registry getRegistry() { return this.registry; }
  
  public String getVMAllocatedPath() { return this.vmAllocatedPath ; }
  
  public VMDescriptor getVMDescriptor() throws RegistryException { 
    VMDescriptor descriptor = registry.getDataAs(vmAllocatedPath, VMDescriptor.class) ;
    return descriptor;
  }
  
  public void addCommandWatcher(NodeWatcher watcher) throws RegistryException {
    Node commandNode = registry.get(vmAllocatedPath + "/commands") ;
    commandNode.watchChildren(watcher);
  }
  
  public void createHeartbeat() throws RegistryException {
    registry.create(vmAllocatedPath + "/status/heartbeat", NodeCreateMode.EPHEMERAL) ;
  }
  
  public void deleteHeartbeat() throws RegistryException {
    registry.delete(vmAllocatedPath + "/status/heartbeat") ;
  }
  
  public void updateStatus(VMStatus status) throws RegistryException {
    registry.setData(vmAllocatedPath + "/status", status) ;
  }
}