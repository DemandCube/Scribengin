package com.neverwinterdp.scribengin.dataflow;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;

public class DataflowContainer {
  private Injector container;

  public DataflowContainer() {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/scribengin/v2") ;
    container = Guice.createInjector(new AppModule(props));
  }
  
  public <T> T getInstance(Class<T> type) { return container.getInstance(type); }
  
  
}
