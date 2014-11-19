package com.neverwinterdp.scribengin.vmresource.jvm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.vmresource.VMResource;
import com.neverwinterdp.scribengin.vmresource.VMResourceService;

public class VMResourceServiceImpl implements VMResourceService {
  private Registry registry;
  private AtomicLong idTracker = new AtomicLong();
  private Map<Long, VMResourceImpl> vmResources = new HashMap<Long, VMResourceImpl>() ;

  @Override
  synchronized public void start() throws Exception {
  }
  
  @Override
  synchronized public void stop() throws Exception {
    for(VMResource vmResource : vmResources.values()) {
      vmResource.exit();
    }
    vmResources.clear();
  }
  
  @Override
  synchronized public VMResource[] getAllocatedVMResources() {
    VMResource[] array = new VMResource[vmResources.size()];
    vmResources.values().toArray(array);
    return array;
  }

  @Override
  synchronized public VMResource allocate(int cpuCores, int memory) {
    VMResourceImpl vmResource = new VMResourceImpl(idTracker.incrementAndGet(), cpuCores, memory);
    vmResources.put(vmResource.getDescriptor().getId(), vmResource);
    return vmResource;
  }

  @Override
  synchronized public void release(VMResource vmResource) throws Exception {
    VMResource found = vmResources.get(vmResource.getDescriptor().getId());
    if(found !=  vmResource) {
      throw new Exception("the vm resource allocator does not manage the VMResource " + vmResource.getDescriptor().getId());
    }
    vmResources.remove(vmResource.getDescriptor().getId()) ;
    vmResource.exit();
  }
}
