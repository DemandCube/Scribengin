package com.neverwinterdp.vm.jvm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMRegistry;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.VMServicePlugin;

@Singleton
public class JVMVMServicePlugin implements VMServicePlugin {
  static private Map<String, VM> vms = new ConcurrentHashMap<String, VM>() ;
  
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
    VM vm = new VM(vmDescriptor);
    Registry newRegistry = vmService.getRegistry().newRegistry().connect();
    VMRegistry vmRegistry = new VMRegistry(newRegistry, vmDescriptor.getStoredPath());
    vm.connect(vmRegistry);
    vms.put(vmDescriptor.getId(), vm);
    return vmDescriptor;
  }

  @Override
  synchronized public void onRelease(VMService vmService, VMDescriptor vmDescriptor) throws Exception {
    VM found = vms.get(vmDescriptor.getId());
    if(found == null) return;
    vms.remove(vmDescriptor.getId()) ;
    found.exit();
  }
  
  static public VM getVM(VMDescriptor descriptor) {
    return vms.get(descriptor.getId());
  }
}