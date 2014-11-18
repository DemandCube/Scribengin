package com.neverwinterdp.scribengin.client;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.master.MasterDescriptor;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.registry.RegistryException;

public class RegistryClient {
  private Registry registry;

  public RegistryClient(Registry registry) {
    this.registry = registry;
  }
  
  public List<MasterDescriptor> getScribenginMasterDescriptors() throws RegistryException {
    List<MasterDescriptor> holder = new ArrayList<MasterDescriptor>();
    List<String> nodes = registry.getChildren("/master");
    for(int i = 0; i < nodes.size(); i++) {
      String name = nodes.get(i) ;
      Node node = registry.get("/master/" + name) ;
      MasterDescriptor descriptor = node.getData(MasterDescriptor.class);
      holder.add(descriptor);
    }
    return holder ;
  }
}
