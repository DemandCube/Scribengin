package com.neverwinterdp.scribengin.registry;

import com.beust.jcommander.Parameter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class RegistryConfig {
  @Parameter(names = {"--registry-factory"}, description = "The factory class to create the RegistryService")
  private String factory ;
  
  @Inject @Named("registry.connect")
  @Parameter(names = {"--registry-connect"}, description = "Connect address string")
  private String connect ;
  
  @Inject @Named("registry.db-domain")
  @Parameter(names = {"--registry-db-domain"}, description = "the location or partion of the database that store the registry")
  private String dbDomain ;
 
  public String getFactory() { return factory; }
  public void setFactory(String factory) { this.factory = factory; }
  
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