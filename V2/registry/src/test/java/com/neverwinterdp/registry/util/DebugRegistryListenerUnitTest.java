package com.neverwinterdp.registry.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.registry.util.NodeFormater;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class DebugRegistryListenerUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TEST_DEBUG_PATH  = "/debug" ;
  
  static private EmbededZKServer zkServerLauncher ;
  
  private Injector container ;
  private Registry registry ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Before
  public void setup() throws Exception {
    AppModule module = new AppModule(new HashMap<String, String>()) {
      @Override
      protected void configure(Map<String, String> properties) {
        bindInstance(RegistryConfig.class, RegistryConfig.getDefault());
        bindType(Registry.class, RegistryImpl.class);
      }
    };
    container = 
      Guice.createInjector(Stage.PRODUCTION, new CloseableModule(), new Jsr250Module(), module);
    registry = container.getInstance(Registry.class);
  }
  
  @After
  public void teardown() throws Exception {
    registry.disconnect();
    container.getInstance(CloseableInjector.class).close();
  }

  @Test
  public void testRegistryDebugListener() throws Exception {
    RegistryDebugger debugger = new RegistryDebugger(System.out, registry) ;
    
    debugger.watch(TEST_DEBUG_PATH, new HelloChildrenDebugger(), true);

    Node debugNode = registry.createIfNotExist(TEST_DEBUG_PATH) ;
    Node hello1 = debugNode.createChild("hello1", new HelloBean("hello - 1"), NodeCreateMode.PERSISTENT);
    Thread.sleep(100);
    Node hello2 = debugNode.createChild("hello2", new HelloBean("hello - 2"), NodeCreateMode.PERSISTENT);
    
    Thread.sleep(100);
    hello1.delete();
    debugNode.rdelete();
    Thread.sleep(2000);
  }
  
  static public class HelloChildrenDebugger implements NodeDebugger {

    @Override
    public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
      NodeFormater formater = new NodeFormater.NodeDumpFormater(node, "") ;
      registryDebugger.watchChildren(node.getPath(), formater, true, true);
    }

    @Override
    public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
    }

    @Override
    public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
      System.out.println("Delete node " + node.getPath()) ;
    }
  }
  
  static public class HelloBean {
    private String hello ;
    
    public HelloBean() {} 
    
    public HelloBean(String hello) {
      this.hello = hello ;
    }

    public String getHello() { return hello; }
    public void setHello(String hello) { this.hello = hello; }
  }
}