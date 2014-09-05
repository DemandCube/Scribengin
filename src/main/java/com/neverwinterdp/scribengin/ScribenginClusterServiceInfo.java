package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.service.ServiceInfo;

public class ScribenginClusterServiceInfo extends ServiceInfo{
  //@Inject @Named("scribengin:exampleParam")
  //private int port = 8080 ;
  
  @Inject(optional = true) @Named("scribengin:ExampleParam2")
  private String exampleParam = null;
  
  //public int getPort() { return port; }
  public String getExampleParam(){return exampleParam; }
}
