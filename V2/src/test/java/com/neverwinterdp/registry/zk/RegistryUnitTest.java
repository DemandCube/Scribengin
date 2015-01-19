package com.neverwinterdp.registry.zk;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
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
    zkServerLauncher.shutdown();
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
  
  @Test
  public void testExistsWatcher() throws Exception {
    String watchPath = "/node/watch" ;
    final CountDownLatch existsSignal = new CountDownLatch(1);
    Registry registry = newRegistry().connect(); 
    registry.watchExists(watchPath, new NodeWatcher() {
      @Override
      public void process(NodeEvent event) {
        existsSignal.countDown();
      }
    });
    registry.createIfNotExist(watchPath);
    existsSignal.await(1, TimeUnit.SECONDS);
    Assert.assertEquals(0,existsSignal.getCount());
    registry.disconnect();
  }
  
  
  @Test
  public void testModifyWatcher() throws Exception {
    final String path = "/node/exists" ;
    final CountDownLatch modifySignal = new CountDownLatch(2);
    final Registry registry = newRegistry().connect(); 
    registry.createIfNotExist(path);
    NodeWatcher watcher = new NodeWatcher() {
      @Override
      public void process(NodeEvent event) {
        modifySignal.countDown();
        System.out.println("got event.....");
      }
    };
    registry.watchModify(path, watcher);
    registry.setData(path, new byte[10]);
    //the watcher suppose to trigger once
    registry.setData(path, new byte[10]);
    modifySignal.await(500, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, modifySignal.getCount());
    
    registry.disconnect();
  }
  
  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
  
  public class NodeEventCatcher extends NodeWatcher {
    private NodeEvent nodeEvent ;
    
    public void process(NodeEvent event) {
      this.nodeEvent = event ;
    }
    
    public NodeEvent getNodeEvent() { return this.nodeEvent ; }
  }
}
