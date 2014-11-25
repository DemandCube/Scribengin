package com.neverwinterdp.vm;

public class VMConfig {
  private String   name;
  private String[] roles ;
  private int      requestCpuCores;
  private int      requestMemory;
  private String   description;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public String[] getRoles() { return roles; }
  public void setRoles(String[] roles) { this.roles = roles; }
  
  public int getRequestCpuCores() { return requestCpuCores; }
  public void setRequestCpuCores(int requestCpuCores) { this.requestCpuCores = requestCpuCores; }
  
  public int getRequestMemory() { return requestMemory; }
  public void setRequestMemory(int requestMemory) { this.requestMemory = requestMemory; }
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
}
