package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
  public VMConfig setName(String name) { 
    this.name = name;
    return this;
  }
  
  public List<String> getRoles() { return roles; }
  
  public VMConfig setRoles(List<String> roles) { 
    this.roles = roles; 
    return this;
  }
  
  public VMConfig addRoles(String ... role) {
    if(roles == null) roles = StringUtil.toList(role);
    else StringUtil.addList(roles, role);
    return this;
  }
  
  public int getRequestCpuCores() { return requestCpuCores; }
  public VMConfig setRequestCpuCores(int requestCpuCores) { 
    this.requestCpuCores = requestCpuCores; 
    return this;
  }
  
  public int getRequestMemory() { return requestMemory; }
  public VMConfig setRequestMemory(int requestMemory) { 
    this.requestMemory = requestMemory; 
    return this;
  }
  
  public String getLocalHome() { return localHome;}
  public VMConfig setLocalHome(String localHome) { 
    this.localHome = localHome;
    return this;
  }
  
  public String getDfsHome() { return dfsHome; }
  public VMConfig setDfsHome(String dfsHome) { 
    this.dfsHome = dfsHome;
    return this;
  }
  
  public RegistryConfig getRegistryConfig() { return registryConfig;}
  public VMConfig setRegistryConfig(RegistryConfig registryConfig) { 
    this.registryConfig = registryConfig;
    return this;
  }
  
  public boolean isSelfRegistration() { return selfRegistration; }
  public VMConfig setSelfRegistration(boolean selfRegistration) { 
    this.selfRegistration = selfRegistration;
    return this;
  }
  
  public String getVmApplication() { return vmApplication;}
  public VMConfig setVmApplication(String vmApplication) { 
    this.vmApplication = vmApplication;
    return this;
  }
  
  public Map<String, String> getProperties() { return properties;}
  public VMConfig setProperties(Map<String, String> appProperties) { 
    this.properties = appProperties;
    return this;
  }
  
  public VMConfig addProperty(String name, String value) {
    if(properties == null) properties = new HashMap<String, String>();
    properties.put(name, value);
    return this;
  }
  
  public Map<String, String> getYarnConf() { return yarnConf; }
  public VMConfig setYarnConf(Map<String, String> yarnConf) {
    this.yarnConf = yarnConf;
    return this;
  }
  
  public VMConfig addYarnProperty(String name, String value) {
    if(yarnConf == null) yarnConf = new HashMap<String, String>();
    yarnConf.put(name, value);
    return this;
  }
  
  public VMConfig addYarnProperty(Map<String, String> conf) {
    if(yarnConf == null) yarnConf = new HashMap<String, String>();
    Iterator<Map.Entry<String, String>> i = conf.entrySet().iterator();
    while(i.hasNext()) {
      Map.Entry<String, String> entry = i.next();
      String key = entry.getKey();
      String value = conf.get(key);
      if(value.length() == 0) continue;
      else if(value.indexOf(' ') > 0) continue;
      yarnConf.put(key, value);
    }
    return this;
  }
  
  
  public void overrideYarnConfiguration(Configuration aconf) {
    for(Map.Entry<String, String> entry : yarnConf.entrySet()) {
      aconf.set(entry.getKey(), entry.getValue()) ;
    }
  }
  
  public String getDescription() { return description; }
  public VMConfig setDescription(String description) { 
    this.description = description; 
    return this;
  }
  
  public boolean isMiniClusterEnv() { return miniClusterEnv; }
  public VMConfig setMiniClusterEnv(boolean miniClusterEnv) {
    this.miniClusterEnv = miniClusterEnv;
    return this;
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
