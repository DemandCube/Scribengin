package com.neverwinterdp.registry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.SequenceNumberTrackerService;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class SequenceNumberTrackerServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  private EmbededZKServer zkServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Test
  public void testSequenceNumberTrackerService() throws Exception {
    final RegistryImpl registry = newRegistry();
    registry.connect();
    SequenceNumberTrackerService service = new SequenceNumberTrackerService(registry);
    service.createIntTrackerIfNotExist("test");
    Assert.assertEquals(1, service.nextInt("test"));
    Assert.assertEquals(2, service.nextInt("test"));
    registry.disconnect();
  }
  
  private RegistryImpl newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
}
