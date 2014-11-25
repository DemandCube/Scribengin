package com.neverwinterdp.scribengin.client;

import java.util.List;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.master.MasterDescriptor;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMServiceRegistry;
import com.neverwinterdp.vm.client.VMClient;

public class ScribenginClient {
  private Registry registry;
  private VMClient vmClient ;

  public ScribenginClient(Registry registry) {
    this.registry = registry;
    this.vmClient = new VMClient(registry) ;
  }

  public Registry getRegistry() { return this.registry; }
  
  public VMClient getVMClient() { return this.vmClient; }
  
  public List<MasterDescriptor> getScribenginMasterDescriptors() throws RegistryException {
    return registry.getChildrenAs("/master", MasterDescriptor.class) ;
  }
  
  public List<VMDescriptor> getVMResourceDescriptors() throws RegistryException {
    return registry.getChildrenAs(VMServiceRegistry.ALLOCATED_PATH, VMDescriptor.class) ;
  }
}
