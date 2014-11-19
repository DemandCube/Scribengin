package com.neverwinterdp.scribengin.vmresource;

public class VMResourceDescriptor {
  private long   id ;
  private int    memory ;
  private int    cpuCores;
  private String hostname;
  
  public long getId() { return id;}
  public void setId(long id) { this.id = id; }
  
  public int getMemory() { return memory; }
  public void setMemory(int memory) { this.memory = memory; }
  
  public int getCpuCores() { return cpuCores; }
  public void setCpuCores(int cpuCores) { this.cpuCores = cpuCores; }
  
  public String getHostname() { return hostname; }
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }
}
