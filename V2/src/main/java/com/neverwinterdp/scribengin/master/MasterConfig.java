package com.neverwinterdp.scribengin.master;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.registry.RegistryConfig;
import com.neverwinterdp.scribengin.vmresource.VMResourceConfig;

public class MasterConfig {
  @ParametersDelegate
  private RegistryConfig registryConfig = new RegistryConfig();
  
  @ParametersDelegate
  private VMResourceConfig vmResourceConfig = new VMResourceConfig();

  public RegistryConfig getRegistryConfig() { return registryConfig; }
  public void setRegistryConfig(RegistryConfig registry) { this.registryConfig = registry; }
  
  public VMResourceConfig getVmResourceConfig() { return vmResourceConfig; }
  public void setVmResourceConfig(VMResourceConfig vmResourceConfig) {
    this.vmResourceConfig = vmResourceConfig;
  }
}
