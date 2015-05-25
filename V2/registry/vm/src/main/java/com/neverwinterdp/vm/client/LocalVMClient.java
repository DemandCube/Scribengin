package com.neverwinterdp.vm.client;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.environment.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class LocalVMClient extends VMClient {
  public LocalVMClient() throws RegistryException {
    this(new RegistryImpl(RegistryConfig.getDefault()));
  }
  
  public LocalVMClient(Registry registry) {
    super(registry);
  }
  
  @Override
  public void createVMMaster(String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(name).
      addRoles("vm-master").
      setSelfRegistration(true).
      setVmApplication(VMServiceApp.class.getName()).
      addProperty("implementation:" + VMServicePlugin.class.getName(), JVMVMServicePlugin.class.getName()).
      setRegistryConfig(getRegistry().getRegistryConfig());
    configureEnvironment(vmConfig);
    VM vm = VM.run(vmConfig);
  }
  
  public void configureEnvironment(VMConfig vmConfig) {
    vmConfig.setEnvironment(VMConfig.Environment.JVM);
  }

}
