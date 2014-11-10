package com.neverwinterdp.scribengin.source.kafka;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.dependency.KafkaServerLauncher;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class KafkaSourceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  private ZookeeperServerLauncher zkServerLauncher ;
  private KafkaServerLauncher     kafkaServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
    
    kafkaServerLauncher = new KafkaServerLauncher(1, "./build/data/kafka") ;
    kafkaServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    kafkaServerLauncher.stop();
    zkServerLauncher.stop();
  }
  
  @Test
  public void testSetup() throws Exception {
    System.out.println("TODO: should insert some data into kafka here");
    Thread.sleep(3000);
  }
  
  @Test
  public void testOldV1CommitLog() throws Exception {
    //TODO: 
    // 1. Copy the old commit log classes and test here.
    // 2. Note that the test should use the current zookeeper and kafka server. The old test need to launch 
    //    the mini cluster which is not right. For FileSystem and dfs FileSytem simulation , there is no need to launch 
    //    the mini cluster
    Thread.sleep(1000);
  }
}
