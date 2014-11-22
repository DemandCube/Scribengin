package com.neverwinterdp.vm.app;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class VMApplication {
  @Inject
  private VMApplicationRegistryService vmAppRegistryService;

  public void onInit() throws Exception {
  }
  
  public void onDestroy() throws Exception {
  }
}
