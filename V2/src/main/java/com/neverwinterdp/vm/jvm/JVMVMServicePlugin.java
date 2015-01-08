package com.neverwinterdp.vm.jvm;

import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.VMServicePlugin;

@Singleton
public class JVMVMServicePlugin implements VMServicePlugin {
  
  @Override
  synchronized public void allocate(VMService vmService, VMConfig vmConfig) throws RegistryException, Exception {
    VM vm = new VM(vmConfig);
    vm.run();
    VM.trackVM(vm);
  }

  @Override
  synchronized public void onKill(VMService vmService, VMDescriptor vmDescriptor) throws Exception {
    VM found = VM.getVM(vmDescriptor);
    if(found == null) return;
    found.shutdown();
  }
}