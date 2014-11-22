package com.neverwinterdp.vm.jvm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMRegistryService;
import com.neverwinterdp.vm.VMService;

@Singleton
public class VMServiceImpl implements VMService {
  @Inject
  private VMConfig config;
  
  @Inject
  private VMRegistryService registryService;
  
  private AtomicLong idTracker = new AtomicLong();
  private Map<Long, VMImpl> vmResources = new HashMap<Long, VMImpl>() ;

  public VMServiceImpl() {
  }
  
  @Override
  synchronized public void start() throws Exception {
  }
  
  @Override
  synchronized public void stop() throws Exception {
    for(VM vmResource : vmResources.values()) {
      vmResource.exit();
    }
    vmResources.clear();
  }
  
  @Override
  synchronized public VM[] getAllocatedVMResources() {
    VM[] array = new VM[vmResources.size()];
    vmResources.values().toArray(array);
    return array;
  }

  @Override
  synchronized public VM allocate(int cpuCores, int memory) throws RegistryException, Exception {
    VMImpl vmResource = new VMImpl(idTracker.incrementAndGet(), cpuCores, memory);
    vmResources.put(vmResource.getDescriptor().getId(), vmResource);
    registryService.allocated(vmResource.getDescriptor());
    return vmResource;
  }

  @Override
  synchronized public void release(VM vmResource) throws Exception {
    VM found = vmResources.get(vmResource.getDescriptor().getId());
    if(found !=  vmResource) {
      throw new Exception("the vm resource allocator does not manage the VM " + vmResource.getDescriptor().getId());
    }
    vmResources.remove(vmResource.getDescriptor().getId()) ;
    vmResource.exit();
    registryService.release(vmResource.getDescriptor());
  }
}
