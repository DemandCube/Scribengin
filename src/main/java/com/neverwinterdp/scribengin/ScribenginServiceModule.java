package com.neverwinterdp.scribengin;

import com.neverwinterdp.server.service.ServiceModule;

public class ScribenginServiceModule extends ServiceModule {
  @Override
  protected void configure() {  
    bind("SparknginClusterHttoService", ScribenginClusterService.class) ;
  }
}