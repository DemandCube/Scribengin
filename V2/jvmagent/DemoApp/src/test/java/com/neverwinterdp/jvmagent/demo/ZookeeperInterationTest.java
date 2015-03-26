package com.neverwinterdp.jvmagent.demo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class ZookeeperInterationTest {
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
  public void launchZookeeper() throws Exception {
    Thread.sleep(100000000);
  }
}