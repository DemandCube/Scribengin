package com.neverwinterdp.vm;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;

@Singleton
public class VMServiceRegistry {
  final static public String ALLOCATED_PATH = "/vm/allocated";
  
  private Registry rService;
  
  @Inject
  public void init(Registry rService) throws Exception {
    this.rService = rService;
    rService.createIfNotExist(ALLOCATED_PATH) ;
  }
  
  public Registry getRegistryService() { return this.rService; }
  
  public VMDescriptor[] getAllocatedVMDescriptors() throws RegistryException {
    List<String> names = rService.getChildren(ALLOCATED_PATH) ;
    VMDescriptor[] descriptor = new VMDescriptor[names.size()];
    for(int i = 0; i < names.size(); i++) {
      String name = names.get(i) ;
      descriptor[i] = rService.getDataAs(ALLOCATED_PATH + "/" + name, VMDescriptor.class) ;
    }
    return descriptor;
  }
  
  public void allocated(VMDescriptor descriptor) throws Exception {
    Node vmNode = rService.create(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName(), NodeCreateMode.PERSISTENT);
    descriptor.setStoredPath(vmNode.getPath());
    vmNode.setData(descriptor);
    Node statusNode  = vmNode.createChild("status", NodeCreateMode.PERSISTENT);
    Node commandNode = vmNode.createChild("commands", NodeCreateMode.PERSISTENT);
  }
  
  public void release(VMDescriptor descriptor) throws Exception {
    rService.rdelete(descriptor.getStoredPath());
  }
  
}