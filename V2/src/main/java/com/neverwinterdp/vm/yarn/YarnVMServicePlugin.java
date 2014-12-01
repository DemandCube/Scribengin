package com.neverwinterdp.vm.yarn;

import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMRegistry;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.VMServicePlugin;

@Singleton
public class YarnVMServicePlugin implements VMServicePlugin {
  private VMContainerManager containerManager;
  
  public YarnVMServicePlugin(VMContainerManager master) {
    this.containerManager = master;
  }
  
  @Override
  public void onRegisterVM(VMService vmService, VM vm) throws Exception {
    VMDescriptor vmDescriptor = vm.getDescriptor();
    VMRegistry vmRegistry = new VMRegistry(vmService.getRegistry(), vmDescriptor.getStoredPath());
    vm.connect(vmRegistry);
  }
  
  @Override
  public void onUnregister(VMService vmService, VM vm) throws Exception {
  }
  
  @Override
  synchronized public VMDescriptor allocate(VMService vmService, VMDescriptor vmDescriptor) throws RegistryException, Exception {
    vmDescriptor = containerManager.allocate(vmService, vmDescriptor);
    return vmDescriptor;
  }

  @Override
  synchronized public void onRelease(VMService vmService, VMDescriptor vmDescriptor) throws Exception {
    System.out.println("call onRelease() ...................................");
  }
}