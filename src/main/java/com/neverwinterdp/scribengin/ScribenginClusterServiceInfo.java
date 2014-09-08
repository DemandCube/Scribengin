package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.service.ServiceInfo;

public class ScribenginClusterServiceInfo extends ServiceInfo{
  @Inject @Named("scribengin:serverproperties")
  private String serverProps = "server.properties";
  
  public String getServerPropertyFile(){return serverProps; }
}
