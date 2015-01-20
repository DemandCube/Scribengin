package com.neverwinterdp.scribengin.client;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.VMDescriptor;

public class ScribenginClient {
  private Registry registry;

  public ScribenginClient(Registry registry) {
    this.registry = registry;
  }

  public Registry getRegistry() { return this.registry; }
  
  public VMDescriptor getScribenginMaster() throws RegistryException {
    Node node = registry.getRef(ScribenginService.LEADER_PATH);
    VMDescriptor descriptor = node.getData(VMDescriptor.class);
    return descriptor;
  }
  
  public List<DataflowDescriptor> getDataflowDescriptor() throws RegistryException {
    return registry.getChildrenAs(ScribenginService.DATAFLOWS_PATH, DataflowDescriptor.class) ;
  }
}
