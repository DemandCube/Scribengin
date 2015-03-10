package com.neverwinterdp.module;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MBeanServer;
import javax.management.ObjectName;

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
import com.mycila.jmx.annotation.JmxBean;
import com.mycila.jmx.annotation.JmxField;
import com.mycila.jmx.annotation.JmxMethod;
import com.mycila.jmx.annotation.JmxParam;
import com.mycila.jmx.annotation.JmxProperty;

public class AppModuleUnitTest {
  @Test
  public void testModuleMapping() throws Exception {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("hello", "Hello Property") ;
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/scribengin/v2") ;
    
    Injector container1 = 
        Guice.createInjector(Stage.PRODUCTION, new CloseableModule(), new Jsr250Module(), new MycilaJmxModuleExt("test-domain"), new AppModule(props));
    
    Hello hello = container1.getInstance(Hello.class);
    hello.sayHello();
    
    MycilaJMXService service = container1.getInstance(MycilaJMXService.class);
    service.increment(1);
    
    //MycilaJMXService service2 = container2.getInstance(MycilaJMXService.class);
    //service2.increment(1);
    
    
    Assert.assertTrue(container1.getInstance(Pojo.class) == container1.getInstance(Pojo.class));
    Pojo pojo = container1.getInstance(Pojo.class) ;
    Assert.assertEquals("127.0.0.1:2181", pojo.getConnect());
    Assert.assertEquals("/scribengin/v2", pojo.getDbDomain());
    container1.getInstance(CloseableInjector.class).close();
  }
  
  public interface HelloMBean {
    public void sayHello() ;
  }
  
  @Singleton
  static public class Hello implements HelloMBean {
    @Inject @Named("hello")
    private String hello ;
    
    @Inject
    public void registerMBean(MBeanServer server) throws Exception {
      ObjectName oname = new ObjectName("com.neverwinterdp:type=HelloMBean,name=scribengin.Hello");
      server.registerMBean(this, oname);
      System.out.println("Init mbean...................");
    }
    
    @Override
    public void sayHello() {
      System.out.println("Say hello: " + hello);
    }
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
  
  @JmxBean("category=category, subcategory=sub-category, type=test, name=main")
  static public final class MycilaJMXService {
    private String name;
    
    @JmxField
    private int    internalField = 10;

    @JmxProperty
    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @JmxMethod(parameters = { @JmxParam(value = "number", description = "put a big number please !") })
    void increment(int n) { internalField += n; }
  }
}
