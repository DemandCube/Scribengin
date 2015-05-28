package com.neverwinterdp.es;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;

public class Configuration {

  @DynamicParameter(names = "--es:", description = "Elasticsearch properties configuration") 
  Map<String, String> esProperties   = new HashMap<>() ;

  public Map<String, String> getESProperties() { return this.esProperties; }
}