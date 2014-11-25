package com.neverwinterdp.vm;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class VMDescriptor {
  private String   storedPath;
  private int      memory;
  private int      cpuCores;
  private String   hostname;
  private VMConfig vmConfig;

  @JsonIgnore
  public String getId() { return vmConfig.getName(); }
  
  public String getStoredPath() { return storedPath; }
  public void setStoredPath(String storedPath) { this.storedPath = storedPath; }
  
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
