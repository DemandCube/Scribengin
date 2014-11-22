package com.neverwinterdp.vm;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.NodeCreateMode;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.scribengin.registry.RegistryService;

@Singleton
public class VMRegistryService {
  final static public String ALLOCATED_PATH = "/vm-resources/allocated";
  
  private RegistryService rService;
  
  @Inject
  public void init(RegistryService rService) throws Exception {
    this.rService = rService;
    rService.createIfNotExist(ALLOCATED_PATH) ;
  }
  
  public VMDescriptor[] getAllocatedVMResources() throws RegistryException {
    List<String> names = rService.getChildren(ALLOCATED_PATH) ;
    VMDescriptor[] descriptor = new VMDescriptor[names.size()];
    for(int i = 0; i < names.size(); i++) {
      String name = names.get(i) ;
      descriptor[i] = rService.getDataAs(ALLOCATED_PATH + "/" + name, VMDescriptor.class) ;
    }
    return descriptor;
  }
  
  public void allocated(VMDescriptor descriptor) throws Exception {
    Node node = rService.create(ALLOCATED_PATH + "/", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    descriptor.setStoredPath(node.getPath());
    node.setData(descriptor);
  }
  
  public void release(VMDescriptor descriptor) throws Exception {
    rService.delete(descriptor.getStoredPath());
  }
  
}
