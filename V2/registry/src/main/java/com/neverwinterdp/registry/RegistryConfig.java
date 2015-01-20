package com.neverwinterdp.registry;

import com.beust.jcommander.Parameter;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.zk.RegistryImpl;

@Singleton
public class RegistryConfig {
  @Parameter(names = "--registry-connect", description = "The registry connect string")
  //@Inject @Named("registry.connect")
  private String connect ;
  
  @Parameter(names = "--registry-db-domain", description = "The registry partition or table")
  //@Inject @Named("registry.db-domain")
  private String dbDomain ;
  
  @Parameter(names = "--registry-implementation", description = "The registry implementation class")
  //@Inject @Named("registry.implementation")
  private String registryImplementation ;
  
  public String getConnect() { return connect; }
  public void setConnect(String connect) { this.connect = connect; }
  
  public String getDbDomain() { return dbDomain; }
  public void setDbDomain(String dbDomain) { this.dbDomain = dbDomain; }
  
  public String getRegistryImplementation() { return registryImplementation; }
  public void setRegistryImplementation(String registryImplementation) { 
    this.registryImplementation = registryImplementation;
  }
  
  static public RegistryConfig getDefault() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("127.0.0.1:2181");
    config.setDbDomain("/NeverwinterDP");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return config ;
  }
}