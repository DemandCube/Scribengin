package com.neverwinterdp.jvmagent.demo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class ZookeeperInterationTest {
  private ZookeeperServerLauncher zkServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
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