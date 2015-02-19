package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.Stage;
import com.google.inject.name.Named;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;

public class AppModuleUnitTest {
  @Test
  public void testModuleMapping() {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/scribengin/v2") ;
    Injector container = 
        Guice.createInjector(Stage.PRODUCTION, new CloseableModule(), new Jsr250Module(), new AppModule(props));
    Assert.assertTrue(container.getInstance(Pojo.class) == container.getInstance(Pojo.class));
    Pojo pojo = container.getInstance(Pojo.class) ;
    Assert.assertEquals("127.0.0.1:2181", pojo.getConnect());
    Assert.assertEquals("/scribengin/v2", pojo.getDbDomain());
    container.getInstance(CloseableInjector.class).close();
  }
  
  @Singleton
  static public class Pojo {
    @Inject @Named("registry.connect")
    private String connect;
    
    @Inject @Named("registry.db-domain")
    private String dbDomain;
    
    @PostConstruct
    public void onInit() {
      System.out.println("onInit() ..........................................") ;
    }
    
    @PreDestroy
    public void onDestroy() {
      System.out.println("onDestroy() ..........................................") ;
    }
    
    public String getConnect() { return connect;}
    public void setConnect(String connect) {  this.connect = connect; }

    public String getDbDomain() { return dbDomain; }
    public void setDbDomain(String dbDomain) { this.dbDomain = dbDomain; }
  }
}
