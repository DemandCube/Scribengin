package com.neverwinterdp.vm.jvm;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.VMServiceRegistry;
import com.neverwinterdp.vm.client.VMClient;

@Singleton
public class VMServiceImpl implements VMService {
  @Inject
  private VMServiceRegistry vmServiceRegistry;
  
  private Map<String, VMImpl> vms = new HashMap<String, VMImpl>() ;

  private VMClient vmClient;
  
  @Inject
  public void init(Registry registry) {
    vmClient = new VMClient(registry) ;
  }
  
  @Override
  public VMClient getVMClient() { return vmClient; }
  
  @Override
  synchronized public VMDescriptor[] getAllocatedVMs() {
    VMDescriptor[] array = new VMDescriptor[vms.size()];
    int i = 0 ;
    for(VM vm : vms.values()) {
      array[i++] = vm.getDescriptor();
    }
    return array;
  }

  @Override
  synchronized public VMDescriptor allocate(VMConfig vmConfig) throws RegistryException, Exception {
    VMDescriptor descriptor = new VMDescriptor() ;
    descriptor.setVmConfig(vmConfig);
    descriptor.setCpuCores(vmConfig.getRequestCpuCores());
    descriptor.setMemory(vmConfig.getRequestMemory());
    vmServiceRegistry.allocated(descriptor);
    RegistryConfig config = vmServiceRegistry.getRegistryService().getRegistryConfig();
    String[] args = {
        "-Pregistry.connect=" + config.getConnect(),
        "-Pregistry.db-domain=" + config.getDbDomain(),
        "-Pregistry.implementation=" + config.getImplementation(),
        "-Pvm.registry.allocated.path=" + descriptor.getStoredPath()
    };
    VMImpl vm = VMImpl.create(args);
    vms.put(vm.getDescriptor().getId(), vm);
    
    return descriptor;
  }

  @Override
  synchronized public void release(VMDescriptor vmDescriptor) throws Exception {
    VM found = vms.get(vmDescriptor.getId());
    if(found == null) {
      throw new Exception("the vm resource allocator does not manage the VM " + vmDescriptor.getId());
    }
    vms.remove(vmDescriptor.getId()) ;
    found.exit();
    vmServiceRegistry.release(vmDescriptor);
  }
  
  synchronized public void appStart(VMDescriptor descriptor, String vmAppClass, String[] args) throws Exception {
    VM found = vms.get(descriptor.getId());
    if(found == null) {
      throw new Exception("the vm resource allocator does not manage the VM " + descriptor.getId());
    }
    found.appStart(vmAppClass, args);
  }
  
  synchronized public void appStop(VMDescriptor descriptor) throws Exception {
    VM found = vms.get(descriptor.getId());
    if(found == null) {
      throw new Exception("the vm resource allocator does not manage the VM " + descriptor.getId());
    }
    found.appStop();
  }
  
  @Override
  synchronized public void start() throws Exception {
    
  }
  
  @Override
  synchronized public void shutdown() throws Exception {
    for(VM vmResource : vms.values()) {
      vmResource.exit();
    }
    vms.clear();
  }
  
  public VMImpl getVM(VMDescriptor descriptor) {
    return this.vms.get(descriptor.getId());
  }
}
