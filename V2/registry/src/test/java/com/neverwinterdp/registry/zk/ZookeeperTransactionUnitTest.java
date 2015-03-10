package com.neverwinterdp.registry.zk;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class ZookeeperTransactionUnitTest {
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
  public void testTransaction() throws Exception {
    zkClient.create("/transaction", "transaction".getBytes(), OPEN_ACL, CreateMode.PERSISTENT) ;
    Transaction transaction = zkClient.transaction();
    transaction.create("/transaction/test", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    transaction.create("/transaction/test/nested", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    transaction.create("/transaction/test/delete", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    transaction.delete("/transaction/test/delete", 0);
    Assert.assertNull(zkClient.exists("/transaction/test", false));
    Assert.assertNull(zkClient.exists("/transaction/test/nested", false));
    transaction.commit();
    Assert.assertNotNull(zkClient.exists("/transaction/test", false));
    Assert.assertNotNull(zkClient.exists("/transaction/test/nested", false));
    Assert.assertNull(zkClient.exists("/transaction/test/delete", false));
  }
  
  @Test
  public void testInvalidTransactionOperation() throws Exception {
    zkClient.create("/transaction", "transaction".getBytes(), OPEN_ACL, CreateMode.PERSISTENT) ;
    Transaction transaction = zkClient.transaction();
    transaction.create("/transaction/good", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    transaction.create("/transaction/bad/nested", new byte[0], OPEN_ACL, CreateMode.PERSISTENT);
    KeeperException expectError = null;
    try {
      transaction.commit();
    } catch(KeeperException ex) {
      expectError = ex ;
    }
    Assert.assertNotNull(expectError);
    Assert.assertTrue(expectError instanceof KeeperException.NoNodeException);
    Assert.assertNull(zkClient.exists("/transaction/good", false));
  }
}