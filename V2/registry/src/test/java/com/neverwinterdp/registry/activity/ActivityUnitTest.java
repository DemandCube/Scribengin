package com.neverwinterdp.registry.activity;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class ActivityUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TEST_PATH = "/node/event" ;
  static private EmbededZKServer zkServerLauncher ;

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
    registry = newRegistry().connect();
  }
  
  @After
  public void teardown() throws Exception {
    registry.delete(TEST_PATH);
    registry.disconnect();
  }

  @Test
  public void testActivity() throws Exception {
  }
  
  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
}