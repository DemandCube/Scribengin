package com.neverwinterdp.es;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.RegistryConfig;

public class Configuration {
  @ParametersDelegate RegistryConfig      registryConfig = RegistryConfig.getDefault();

  @DynamicParameter(names = "--es:", description = "Elasticsearch properties configuration") Map<String, String> esProperties   = new HashMap<>() ;

  public RegistryConfig getRegistryConfig() { return this.registryConfig; }
  
  public Map<String, String> getESProperties() { return this.esProperties; }
}