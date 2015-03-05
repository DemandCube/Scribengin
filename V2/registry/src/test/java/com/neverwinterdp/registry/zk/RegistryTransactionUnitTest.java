package com.neverwinterdp.registry.zk;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class RegistryTransactionUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
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
  public void testTransaction() throws Exception {
    final Registry registry = newRegistry().connect(); 
    registry.create("/transaction", "transaction".getBytes(), NodeCreateMode.PERSISTENT) ;
    Transaction transaction = registry.getTransaction();
    transaction.create("/transaction/test", new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create("/transaction/test/nested", new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create("/transaction/test/delete", new byte[0], NodeCreateMode.PERSISTENT);
    transaction.delete("/transaction/test/delete");
    Assert.assertFalse(registry.exists("/transaction/test"));
    Assert.assertFalse(registry.exists("/transaction/test/nested"));
    transaction.commit();
    Assert.assertTrue(registry.exists("/transaction/test"));
    Assert.assertTrue(registry.exists("/transaction/test/nested"));
    Assert.assertFalse(registry.exists("/transaction/test/delete"));
    registry.disconnect();
  }
  
  @Test
  public void testInvalidTransactionOperation() throws Exception {
    final Registry registry = newRegistry().connect(); 
    registry.create("/transaction", "transaction".getBytes(), NodeCreateMode.PERSISTENT) ;
    Transaction transaction = registry.getTransaction();
    transaction.create("/transaction/good", new byte[0], NodeCreateMode.PERSISTENT);
    transaction.create("/transaction/bad/nested", new byte[0], NodeCreateMode.PERSISTENT);
    RegistryException expectError = null;
    try {
      transaction.commit();
    } catch(RegistryException ex) {
      expectError = ex ;
    }
    Assert.assertNotNull(expectError);
    Assert.assertTrue(expectError.getCause() instanceof KeeperException.NoNodeException);
    Assert.assertFalse(registry.exists("/transaction/good"));
    registry.disconnect();
  }
  
  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
}
