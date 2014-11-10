package com.neverwinterdp.scribengin.registry.zk;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.NodeCreateMode;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.util.FileUtil;

public class RegistryUnitTest {
  private ZookeeperServerLauncher zkServerLauncher ;
  
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

  @Test
  public void testPersistent() throws Exception {
    String DATA = "hello";
    Registry registry = newRegistry().connect(); 
    Node persistentNode = registry.create("/persistent", DATA.getBytes(), NodeCreateMode.PERSISTENT) ;
    Assert.assertEquals("/persistent", persistentNode.getPath()) ;
    Assert.assertTrue(persistentNode.exists());
    Assert.assertEquals(DATA, new String(persistentNode.getData())) ;
    registry.disconnect();
    
    registry = newRegistry().connect();
    persistentNode = registry.get("/persistent") ;
    Assert.assertTrue(persistentNode.exists());
    Assert.assertEquals(DATA, new String(persistentNode.getData())) ;
    registry.disconnect();
  }
  
  @Test
  public void testPersistentSequential() throws Exception {
    Registry  registry = newRegistry().connect(); 
    
    registry.create("/sequential", NodeCreateMode.PERSISTENT) ;
    Node seqNode1 = registry.create("/sequential/node", NodeCreateMode.PERSISTENT_SEQUENTIAL) ;
    System.out.println("path = " + seqNode1.getPath());
    Assert.assertTrue(seqNode1.getPath().matches("/sequential/node0+")) ;
    registry.disconnect();
  }
  
  @Test
  public void testEphemeral() throws Exception {
    Registry registry = newRegistry().connect(); 
    registry.create("/ephemeral", NodeCreateMode.PERSISTENT) ;
    Node ephemeralNode = registry.create("/ephemeral/node", NodeCreateMode.EPHEMERAL) ;
    Assert.assertEquals("/ephemeral/node", ephemeralNode.getPath()) ;
    Assert.assertTrue(ephemeralNode.exists()) ;
    registry.disconnect();
    
    registry = newRegistry().connect();
    Assert.assertTrue(registry.get("/ephemeral").exists());
    Assert.assertFalse(registry.get(ephemeralNode.getPath()).exists());
    registry.disconnect();
  }
  
  @Test
  public void testEphemeralSequential() throws Exception {
    Registry registry = newRegistry().connect(); 
    registry.create("/ephemeral-sequential", NodeCreateMode.PERSISTENT) ;
    
    Node seqNode = registry.create("/ephemeral-sequential/node", NodeCreateMode.EPHEMERAL_SEQUENTIAL) ;
    Assert.assertTrue(seqNode.getPath().matches("/ephemeral-sequential/node0+")) ;
    Assert.assertTrue(seqNode.exists()) ;
    registry.disconnect();
    
    registry = newRegistry().connect();
    Assert.assertFalse(registry.get(seqNode.getPath()).exists());
    registry.disconnect();
  }
  
  private Registry newRegistry() {
    return new RegistryImpl("127.0.0.1:2181", "/scribengin/v2") ;
  }
}
