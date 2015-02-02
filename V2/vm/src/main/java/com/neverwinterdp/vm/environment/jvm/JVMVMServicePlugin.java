package com.neverwinterdp.vm.environment.jvm;

import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.service.VMService;
import com.neverwinterdp.vm.service.VMServicePlugin;

@Singleton
public class JVMVMServicePlugin implements VMServicePlugin {
  
  @Override
  synchronized public void allocateVM(VMService vmService, VMConfig vmConfig) throws RegistryException, Exception {
    VM vm = new VM(vmConfig);
    vm.run();
    VM.trackVM(vm);
  }

  @Override
  synchronized public void killVM(VMService vmService, VMDescriptor vmDescriptor) throws Exception {
    VM found = VM.getVM(vmDescriptor);
    if(found == null) return;
    found.shutdown();
  }

  @Override
  public void shutdown() {
  }
}