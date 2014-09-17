package com.neverwinterdp.server.module;

import java.util.Map;

import com.neverwinterdp.scribengin.cluster.ScribenginWorkerClusterService;
import com.neverwinterdp.server.module.ModuleConfig;
import com.neverwinterdp.server.module.ServiceModule;

@ModuleConfig(name = "ScribenginWorker", autostart = false, autoInstall=false)
public class ScribenginWorkerServiceModule extends ServiceModule {
  protected void configure(Map<String, String> properties) {  
    bind("ScribenginWorkerClusterService",ScribenginWorkerClusterService.class) ;
  }
}