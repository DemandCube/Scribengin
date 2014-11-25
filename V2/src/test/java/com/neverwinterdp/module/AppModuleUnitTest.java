package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;

public class AppModuleUnitTest {
  @Test
  public void testModuleMapping() {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/scribengin/v2") ;
    props.put("registry.implementation", RegistryImpl.class.getName()) ;
    Injector container = Guice.createInjector(new AppModule(props));
    Assert.assertTrue(container.getInstance(RegistryConfig.class) == container.getInstance(RegistryConfig.class));
    RegistryConfig config = container.getInstance(RegistryConfig.class) ;
    Assert.assertEquals("127.0.0.1:2181", config.getConnect());
    Assert.assertEquals("/scribengin/v2", config.getDbDomain());
  }
}
