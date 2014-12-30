package com.neverwinterdp.registry.zk;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class ZookeeperUnitTest {
  public final Id ANYONE_ID = new Id("world", "anyone");
  public final ArrayList<ACL> OPEN_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID)));
  
  private ZookeeperServerLauncher zkServerLauncher ;
  private ZooKeeper zkClient ;
  static String connection;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
    
    Watcher watcher = new Watcher() {
      public void process(WatchedEvent event) {
        System.out.println("on event: " + event.getPath() + " - " + event.getType() + " - " + event.getState());
      }
    };
    zkClient = new ZooKeeper("127.0.0.1:2181", 15000, watcher);
  }
  
  @After
  public void teardown() throws Exception {
    zkClient.close();
    zkServerLauncher.shutdown();
  }

  @Test
  public void testZkClient() throws Exception {
    String homePath = zkClient.create("/home", "home".getBytes(), OPEN_ACL, CreateMode.PERSISTENT) ;
    Assert.assertEquals("/home", homePath) ;
    Assert.assertEquals("home", new String(zkClient.getData("/home", null, new Stat()))) ;
    
    String sequentialPath = zkClient.create("/sequential", "sequential".getBytes(), OPEN_ACL, CreateMode.PERSISTENT_SEQUENTIAL) ;
    Assert.assertTrue(sequentialPath.matches("/sequential0+1")) ;
    Assert.assertEquals("sequential", new String(zkClient.getData(sequentialPath, null, new Stat()))) ;
  }
}