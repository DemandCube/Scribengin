package com.neverwinterdp.vm;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class VMDescriptor {
  private String   registryPath;
  private int      memory;
  private int      cpuCores;
  private String   hostname;
  private VMConfig vmConfig;

  public VMDescriptor() { }
  
  public VMDescriptor(VMConfig vmConfig) {
    this.vmConfig = vmConfig;
    setCpuCores(vmConfig.getRequestCpuCores());
    setMemory(vmConfig.getRequestMemory());
  }
  
  @JsonIgnore
  public String getId() { return vmConfig.getName(); }
  
  public String getRegistryPath() { return registryPath; }
  public void setRegistryPath(String path) { this.registryPath = path; }
  
  public int getMemory() { return memory; }
  public void setMemory(int memory) { this.memory = memory; }
  
  public int getCpuCores() { return cpuCores; }
  public void setCpuCores(int cpuCores) { this.cpuCores = cpuCores; }
  
  public String getHostname() { return hostname; }
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
  
  public VMConfig getVmConfig() { return vmConfig; }
  public void setVmConfig(VMConfig vmConfig) {
    this.vmConfig = vmConfig;
  }
}
