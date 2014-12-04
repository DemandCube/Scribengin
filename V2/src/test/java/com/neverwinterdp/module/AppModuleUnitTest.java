package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;

public class AppModuleUnitTest {
  @Test
  public void testModuleMapping() {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/scribengin/v2") ;
    Injector container = Guice.createInjector(new AppModule(props));
    Assert.assertTrue(container.getInstance(Pojo.class) == container.getInstance(Pojo.class));
    Pojo pojo = container.getInstance(Pojo.class) ;
    Assert.assertEquals("127.0.0.1:2181", pojo.getConnect());
    Assert.assertEquals("/scribengin/v2", pojo.getDbDomain());
  }
  
  @Singleton
  static public class Pojo {
    @Inject @Named("registry.connect")
    private String connect;
    
    @Inject @Named("registry.db-domain")
    private String dbDomain;

    public String getConnect() { return connect;}
    public void setConnect(String connect) {  this.connect = connect; }

    public String getDbDomain() { return dbDomain; }
    public void setDbDomain(String dbDomain) { this.dbDomain = dbDomain; }
  }
}
