package com.neverwinterdp.scribengin.registry.zk;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.NodeCreateMode;
import com.neverwinterdp.scribengin.registry.NodeEvent;
import com.neverwinterdp.scribengin.registry.NodeEventCatcher;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.util.FileUtil;

public class RegistryUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
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
    zkServerLauncher.stop();
  }

  @Test
  public void testPersistent() throws Exception {
    String DATA = "hello";
    
    Registry registry = newRegistry().connect(); 
    Node pNode = registry.create("/persistent", DATA.getBytes(), NodeCreateMode.PERSISTENT) ;
    Assert.assertEquals("/persistent", pNode.getPath()) ;
    Assert.assertTrue(pNode.exists());
    Assert.assertEquals(DATA, new String(pNode.getData())) ;
    registry.disconnect();
    
    registry = newRegistry().connect();
    pNode = registry.get("/persistent") ;
    Assert.assertTrue(pNode.exists());
    //TODO: should test the the create child , create event , modify data event
    NodeEventCatcher pNodeEventCatcher = new NodeEventCatcher() ;
    pNode.watch(pNodeEventCatcher) ;
    Assert.assertEquals(DATA, new String(pNode.getData())) ;
    pNode.delete();
    Assert.assertFalse(pNode.exists());
    Thread.sleep(1000); //wait to make sure that the watcher is invoked
    Assert.assertEquals(pNode.getPath(), pNodeEventCatcher.getNodeEvent().getPath());
    Assert.assertEquals(NodeEvent.Type.DELETE, pNodeEventCatcher.getNodeEvent().getType());
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
