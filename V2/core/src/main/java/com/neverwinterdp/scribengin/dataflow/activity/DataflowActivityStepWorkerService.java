package com.neverwinterdp.scribengin.dataflow.activity;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityStepWorkerService;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowActivityStepWorkerService extends ActivityStepWorkerService<VMDescriptor> {
  
  @Inject
  public void onInit(VMDescriptor vmDescriptor,
                     Injector container, 
                     DataflowRegistry dataflowRegistry) throws RegistryException {
    init(vmDescriptor, container, dataflowRegistry.getActivitiesPath());
  }
  
}
