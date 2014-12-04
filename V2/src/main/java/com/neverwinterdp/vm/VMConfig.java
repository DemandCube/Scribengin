package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.util.text.StringUtil;

public class VMConfig {
  @Parameter(names = "--name", description = "The registry partition or table")
  private String              name;
  
  @Parameter(names = "--roles", description = "The VM roles")
  private List<String>        roles = new ArrayList<String>();
  
  @Parameter(names = "--cpu-cores", description = "The request number of cpu cores")
  private int                 requestCpuCores = 1;
  @Parameter(names = "--memory", description = "The request amount of memory in MB")
  private int                 requestMemory   = 128;
  
  @Parameter(names = "--local-home", description = "vm local home")
  private String localHome ;
  
  @Parameter(names = "--dfs-home", description = "vm dfs home")
  private String dfsHome ;
  
  @ParametersDelegate
  private RegistryConfig      registryConfig = new RegistryConfig();
  
  @Parameter(names = "--self-registration", description = "Self create the registation entry in the registry, for master node")
  private boolean             selfRegistration = false;
  
  @Parameter(names = "--vm-application", description = "The vm application class")
  private String              vmApplication;

  @DynamicParameter(names = "--prop:", description = "The application configuration properties")
  private Map<String, String> properties   = new HashMap<String, String>();
  
  @DynamicParameter(names = "--yarn:", description = "The application configuration properties")
  private Map<String, String> yarnConf   = new HashMap<String, String>();
  
  @Parameter(names = "--description", description = "Description")
  private String              description;
  
  @Parameter(names = "--mini-cluster-env", description = "Mini cluster env")
  private boolean             miniClusterEnv = false;
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  
  public List<String> getRoles() { return roles; }
  public void setRoles(List<String> roles) { this.roles = roles; }
  public void addRoles(String ... role) {
    if(roles == null) roles = StringUtil.toList(role);
    else StringUtil.addList(roles, role);
  }
  
  public int getRequestCpuCores() { return requestCpuCores; }
  public void setRequestCpuCores(int requestCpuCores) { this.requestCpuCores = requestCpuCores; }
  
  public int getRequestMemory() { return requestMemory; }
  public void setRequestMemory(int requestMemory) { this.requestMemory = requestMemory; }
  
  public String getLocalHome() { return localHome;}
  public void setLocalHome(String localHome) { this.localHome = localHome; }
  
  public String getDfsHome() { return dfsHome; }
  public void setDfsHome(String dfsHome) { this.dfsHome = dfsHome;}
  
  public RegistryConfig getRegistryConfig() { return registryConfig;}
  public void setRegistryConfig(RegistryConfig registryConfig) { this.registryConfig = registryConfig;}
  
  public boolean isSelfRegistration() { return selfRegistration; }
  public void setSelfRegistration(boolean selfRegistration) { this.selfRegistration = selfRegistration; }
  
  public String getVmApplication() { return vmApplication;}
  public void setVmApplication(String vmApplication) { this.vmApplication = vmApplication;}
  
  public Map<String, String> getProperties() { return properties;}
  public void setProperties(Map<String, String> appProperties) { this.properties = appProperties; }
  
  public Map<String, String> getYarnConf() { return yarnConf; }
  public void setYarnConf(Map<String, String> yarnConf) {
    this.yarnConf = yarnConf;
  }
  
  public void overrideYarnConfiguration(Configuration aconf) {
    for(Map.Entry<String, String> entry : yarnConf.entrySet()) {
      aconf.set(entry.getKey(), entry.getValue()) ;
    }
  }
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public boolean isMiniClusterEnv() { return miniClusterEnv; }
  public void setMiniClusterEnv(boolean miniClusterEnv) {
    this.miniClusterEnv = miniClusterEnv;
  }
  
  public String buildCommand() {
    StringBuilder b = new StringBuilder() ;
    b.append("java ").append(" -Xmx" + requestMemory + "m ").append(VM.class.getName()) ;
    addParameters(b);
    System.out.println("Command: " + b.toString());
    return b.toString() ;
  }
  
  private void addParameters(StringBuilder b) {
    if(name != null) {
      b.append(" --name ").append(name) ;
    }
    
    if(roles != null && roles.size() > 0) {
      b.append(" --roles ");
      for(String role : roles) {
        b.append(role).append(" ");
      }
    }
    
    b.append(" --cpu-cores ").append(requestCpuCores) ;
    
    b.append(" --memory ").append(requestMemory) ;
    
    if(localHome != null) {
      b.append(" --local-home ").append(localHome) ;
    }
    
    if(dfsHome != null) {
      b.append(" --dfs-home ").append(dfsHome) ;
    }
    
    b.append(" --vm-application ").append(vmApplication);
    
    if(selfRegistration) {
      b.append(" --self-registration ") ;
    }
    
    b.append(" --registry-connect ").append(registryConfig.getConnect()) ;
    b.append(" --registry-db-domain ").append(registryConfig.getDbDomain()) ;
    b.append(" --registry-implementation ").append(registryConfig.getRegistryImplementation()) ;
    
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      b.append(" --prop:").append(entry.getKey()).append("=").append(entry.getValue()) ;
    }
    
    for(Map.Entry<String, String> entry : yarnConf.entrySet()) {
      b.append(" --yarn:").append(entry.getKey()).append("=").append(entry.getValue()) ;
    }
    
    if(this.miniClusterEnv) {
      b.append(" --mini-cluster-env ") ;
    }
  }
}
