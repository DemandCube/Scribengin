package com.neverwinterdp.scribengin.client;

import java.util.List;

import org.junit.Assert;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.scribengin.service.VMScribenginServiceApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

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
  
  public VMDescriptor createVMMaster(VMClient vmClient, String name) throws Exception {
    throw new RuntimeException("This method is not supported, use the LocalScribenginClient or YarnScribenginClient") ;
  }
  
  public VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(name).
      addRoles("scribengin-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMScribenginServiceApp.class.getName());
    configureEnvironment(vmConfig);
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  protected void configureEnvironment(VMConfig vmConfig) {
    throw new RuntimeException("This method is not supported, use the LocalScribenginClient or YarnScribenginClient") ;
  }
}
