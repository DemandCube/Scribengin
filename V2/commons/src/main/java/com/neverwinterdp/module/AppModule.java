package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class AppModule extends AbstractModule {
  private Map<String, String> properties = new HashMap<String, String>();

  public AppModule(Map<String, String> properties) {
    this.properties = properties;
  }
  
  final protected void configure() {
    configure(properties) ;
    try {
      for(Map.Entry<String, String> entry : properties.entrySet()) {
        String key = entry.getKey();
        if(!key.startsWith("implementation:")) continue;
        String keyClassName = key.substring("implementation:".length()) ;
        bindType(Class.forName(keyClassName), entry.getValue());
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    bindProperties() ;
  }
  
  protected void configure(Map<String, String> properties) {
  }
  
  public <T> void bind(String id, Class<T> type) {
    Key<T> key = Key.get(type, Names.named(id)) ;
    bind(key).to(type).asEagerSingleton(); ;
  }
  
  public <T> void bindType(Class<T> type) {
    Key<T> key = Key.get(type, Names.named(type.getSimpleName())) ;
    bind(key).to(type).asEagerSingleton();
  }
  
  public <T> void bindType(Class<T> type, String impl) throws ClassNotFoundException {
    Class<?> clazz = (Class<?>)Class.forName(impl);
    bind(type).to((Class)clazz);
  }
  
  public <T> void bindType(Class<T> type, Class<? extends T> clazz) {
    bind(type).to(clazz);
  }
  
  public void bindType(String type) throws ClassNotFoundException {
    Class<?> clazz = Class.forName(type);
    bindType(clazz);
  }
  
  public <T> void bindInstance(String id, T instance) {
    Key<T> key = (Key<T>)Key.get(instance.getClass(), Names.named(id)) ;
    bind(key).toInstance(instance); 
  }
  
  public <T> void bindInstance(Class<T> type, T instance) {
    bind(type).toInstance(instance);
  }
  
  void bindProperties() {
    for(Map.Entry<String, String> sel : properties.entrySet()) {
      bindConstant().annotatedWith(Names.named(sel.getKey())).to(sel.getValue());
    }
  }
}