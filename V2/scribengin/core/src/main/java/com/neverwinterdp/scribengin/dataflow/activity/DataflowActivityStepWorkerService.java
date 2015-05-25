package com.neverwinterdp.scribengin.dataflow.activity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityStepWorkerService;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
public class DataflowActivityStepWorkerService extends ActivityStepWorkerService<VMDescriptor> {
  
  @Inject
  public void onInit(VMDescriptor vmDescriptor) throws RegistryException {
    init(vmDescriptor);
  }
  
}
