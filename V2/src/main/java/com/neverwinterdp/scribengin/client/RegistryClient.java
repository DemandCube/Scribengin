package com.neverwinterdp.scribengin.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.scribengin.master.MasterDescriptor;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.RegistryService;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMRegistryService;

public class RegistryClient {
  private RegistryService registry;

  public RegistryClient(RegistryService registry) {
    this.registry = registry;
  }
  
  public List<MasterDescriptor> getScribenginMasterDescriptors() throws RegistryException {
    return getChildrenAs("/master", MasterDescriptor.class) ;
  }
  
  public List<VMDescriptor> getVMResourceDescriptors() throws RegistryException {
    return getChildrenAs(VMRegistryService.ALLOCATED_PATH, VMDescriptor.class) ;
  }
  
  public <T> List<T> getChildrenAs(String path, Class<T> type) throws RegistryException {
    List<T> holder = new ArrayList<T>();
    List<String> nodes = registry.getChildren(path);
    Collections.sort(nodes);
    for(int i = 0; i < nodes.size(); i++) {
      String name = nodes.get(i) ;
      Node node = registry.get(path + "/" + name) ;
      T object = node.getData(type);
      holder.add(object);
    }
    return holder ;
  }
}
