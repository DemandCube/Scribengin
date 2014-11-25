package com.neverwinterdp.registry;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class RegistryConfig {
  @Inject @Named("registry.connect")
  private String connect ;
  
  @Inject @Named("registry.db-domain")
  private String dbDomain ;
  
  @Inject @Named("registry.implementation")
  private String implementation ;
  
  public String getImplementation() { return implementation; }
  public void setImplementation(String impl) { this.implementation = impl; }
  
  public String getConnect() { return connect; }
  public void setConnect(String connect) { this.connect = connect; }
  
  public String getDbDomain() { return dbDomain; }
  public void setDbDomain(String dbDomain) { this.dbDomain = dbDomain; }
  
  static public RegistryConfig getDefault() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("127.0.0.1:2181");
    config.setDbDomain("/NeverwinterDP");
    return config ;
  }
}