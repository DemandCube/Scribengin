package com.neverwinterdp.scribengin.client.shell;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class HadoopProperties extends HashMap<String, String> {
  
  public Configuration getConfiguration() {
    Configuration conf = new Configuration() ;
    overrideConfiguration(conf) ;
    return conf ;
  }
  
  public void overrideConfiguration(Configuration aconf) {
    for(Map.Entry<String, String> entry : entrySet()) {
      aconf.set(entry.getKey(), entry.getValue()) ;
    }
  }
}
