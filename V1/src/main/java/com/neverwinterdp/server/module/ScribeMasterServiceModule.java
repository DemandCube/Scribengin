package com.neverwinterdp.server.module;

import java.util.Map;

import com.neverwinterdp.scribengin.cluster.ScribeMasterClusterService;
import com.neverwinterdp.server.module.ModuleConfig;
import com.neverwinterdp.server.module.ServiceModule;

@ModuleConfig(name = "ScribeMaster", autostart = false, autoInstall=false)
public class ScribeMasterServiceModule extends ServiceModule {
  protected void configure(Map<String, String> properties) {  
    bind("ScribeMasterClusterService",ScribeMasterClusterService.class) ;
  }
}