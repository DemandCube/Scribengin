package com.neverwinterdp.server.module;

import java.util.Map;

import com.neverwinterdp.scribengin.cluster.ScribeConsumerClusterService;
import com.neverwinterdp.server.module.ModuleConfig;
import com.neverwinterdp.server.module.ServiceModule;

@ModuleConfig(name = "ScribeConsumer", autostart = false, autoInstall=false)
public class ScribeConsumerServiceModule extends ServiceModule {
  protected void configure(Map<String, String> properties) {  
    bind("ScribeConsumerClusterService",ScribeConsumerClusterService.class) ;
  }
}