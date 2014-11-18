package com.neverwinterdp.scribengin.registry;

import com.beust.jcommander.Parameter;

public class RegistryConfig {
  @Parameter(names = {"--registry-factory"}, description = "The factory class to create the Registry")
  private String factory ;
  
  @Parameter(names = {"--registry-connect"}, description = "Connect address string")
  private String connect ;
  
  @Parameter(names = {"--registry-db-domain"}, description = "the location or partion of the database that store the registry")
  private String dbLocation ;
 
  public String getFactory() { return factory; }
  public void setFactory(String factory) { this.factory = factory; }
  
  public String getConnect() { return connect; }
  public void setConnect(String connect) { this.connect = connect; }
  
  public String getDbLocation() { return dbLocation; }
  public void setDbLocation(String dbLocation) { this.dbLocation = dbLocation; }
}