package com.neverwinterdp.vm.app;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.scribengin.registry.RegistryService;

@Singleton
public class VMApplicationRegistryService {
  @Inject @Named("vmresource.app.registry.allocated.path")
  private String registryVMAllocatedPath;
  
  @Inject
  private RegistryService registryService ;
  
}
