package com.neverwinterdp.scribengin;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.client.shell.Shell;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.scribengin.module.ScribenginModule;
import com.neverwinterdp.scribengin.registry.RegistryConfig;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.scribengin.registry.RegistryService;
import com.neverwinterdp.scribengin.registry.zk.RegistryServiceImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.app.VMApplication;
import com.neverwinterdp.vm.jvm.VMServiceImpl;

abstract public class ScribenginUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  protected ZookeeperServerLauncher zkServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    zkServerLauncher.stop();
  }

  protected Injector newMasterContainer() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    props.put("registry.implementation", RegistryServiceImpl.class.getName()) ;
    props.put("vmresource.implementation", VMServiceImpl.class.getName()) ;
    ScribenginModule module = new ScribenginModule(props) {
      protected void configure(Map<String, String> properties) {
        try {
          bindType(RegistryService.class, properties.get("registry.implementation"));
          bindType(VMService.class, properties.get("vmresource.implementation"));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };
    return Guice.createInjector(module);
  }
  
  protected <T> Injector newVMApplicationContainer(Class<T> appType) {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    props.put("registry.implementation", RegistryServiceImpl.class.getName()) ;
    props.put("vmapplication.class", appType.getName()) ;
    ScribenginModule module = new ScribenginModule(props) {
      protected void configure(Map<String, String> properties) {
        try {
          bindType(RegistryService.class, properties.get("registry.implementation"));
          bindType(VMApplication.class, properties.get("vmapplication.class"));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };
    return Guice.createInjector(module);
  }
  
  protected RegistryService newRegistry() {
    return new RegistryServiceImpl(RegistryConfig.getDefault());
  }
  
  protected Shell newShell() throws RegistryException {
    return new Shell(newRegistry().connect()) ;
  }
}
