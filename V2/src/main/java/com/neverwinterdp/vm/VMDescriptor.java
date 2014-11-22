package com.neverwinterdp.vm;

public class VMDescriptor {
  private long   id ;
  private String storedPath;
  private int    memory ;
  private int    cpuCores;
  private String hostname;
  private String description;
  
  public long getId() { return id;}
  public void setId(long id) { this.id = id; }
  
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
  
  public String getDescription() { return description; }
  public void setDescription(String description) {
    this.description = description;
  }
}
