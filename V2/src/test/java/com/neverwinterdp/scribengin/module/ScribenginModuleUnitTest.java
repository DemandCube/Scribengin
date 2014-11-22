package com.neverwinterdp.scribengin.module;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.registry.RegistryConfig;

public class ScribenginModuleUnitTest {
  @Test
  public void testModuleMapping() {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/scribengin/v2") ;
    Injector container = Guice.createInjector(new ScribenginModule(props));
    Assert.assertTrue(container.getInstance(RegistryConfig.class) == container.getInstance(RegistryConfig.class));
    RegistryConfig config = container.getInstance(RegistryConfig.class) ;
    Assert.assertEquals("127.0.0.1:2181", config.getConnect());
    Assert.assertEquals("/scribengin/v2", config.getDbDomain());
  }
}
