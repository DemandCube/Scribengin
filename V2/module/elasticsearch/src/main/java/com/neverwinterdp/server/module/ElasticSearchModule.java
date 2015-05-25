package com.neverwinterdp.server.module;

import java.util.Map;

import com.neverwinterdp.es.cluster.ElasticSearchClusterService;
import com.neverwinterdp.server.module.ServiceModule;

@ModuleConfig(name = "ElasticSearch", autostart = false, autoInstall=false)
public class ElasticSearchModule extends ServiceModule {
  
  protected void configure(Map<String, String> properties) {  
    bindService(ElasticSearchClusterService.class) ;
  }
}