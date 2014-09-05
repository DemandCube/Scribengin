package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.service.ServiceInfo;

public class ScribenginClusterServiceInfo extends ServiceInfo{
  
  @Inject(optional = true) @Named("scribengin:example")
  private String example = "";
  
  //public int getPort() { return port; }
  public String getExample(){return example; }
}
