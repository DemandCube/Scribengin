package com.neverwinterdp.registry.zk;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryListener;
import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class RegistryListenerUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  static public String TEST_PATH = "/node/event" ;
  static private ZookeeperServerLauncher zkServerLauncher ;
  
  @BeforeClass
  static public void startServer() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  static public void stopServer() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  private Registry registry ;
  
  @Before
  public void setup() throws Exception {
    registry = newRegistry().connect();
  }
  
  @After
  public void teardown() throws Exception {
    registry.delete(TEST_PATH);
    registry.disconnect();
  }

  @Test
  public void testOneTimeCreateWatch() throws Exception {
    RegistryListener listener = new RegistryListener(registry);
    NodeEventCatcher nodeEventCatcher = new NodeEventCatcher();
    listener.watch(TEST_PATH, nodeEventCatcher, false);
    Node node = registry.createIfNotExist(TEST_PATH) ;
    Thread.sleep(100);
    Assert.assertEquals(0, listener.getWatchers().size());
    Assert.assertEquals(NodeEvent.Type.CREATE, nodeEventCatcher.getNodeEvent().getType());
    Assert.assertEquals(TEST_PATH, nodeEventCatcher.getNodeEvent().getPath());
  }
  
  @Test
  public void testOneTimeModifyWatch() throws Exception {
    RegistryListener listener = new RegistryListener(registry);
    Node node = registry.createIfNotExist(TEST_PATH) ;
    NodeEventCatcher nodeEventCatcher = new NodeEventCatcher();
    listener.watch(TEST_PATH, nodeEventCatcher, false);
    node.setData(new byte[10]);
    Thread.sleep(100);
    Assert.assertEquals(0, listener.getWatchers().size());
    Assert.assertEquals(NodeEvent.Type.MODIFY, nodeEventCatcher.getNodeEvent().getType());
    Assert.assertEquals(TEST_PATH, nodeEventCatcher.getNodeEvent().getPath());
  }
  
  @Test
  public void testPersistentWatch() throws Exception {
    RegistryListener listener = new RegistryListener(registry);
    NodeEventCatcher nodeEventCatcher = new NodeEventCatcher();
    listener.watch(TEST_PATH, nodeEventCatcher, true);
    Node node = registry.createIfNotExist(TEST_PATH) ;
    Thread.sleep(100);
    Assert.assertEquals(1, listener.getWatchers().size());
    Assert.assertEquals(NodeEvent.Type.CREATE, nodeEventCatcher.getNodeEvent().getType());
    Assert.assertEquals(TEST_PATH, nodeEventCatcher.getNodeEvent().getPath());
    
    node.setData(new byte[10]);
    Thread.sleep(100);
    Assert.assertEquals(1, listener.getWatchers().size());
    Assert.assertEquals(NodeEvent.Type.MODIFY, nodeEventCatcher.getNodeEvent().getType());
    Assert.assertEquals(TEST_PATH, nodeEventCatcher.getNodeEvent().getPath());
    
    listener.dump(System.out);
    listener.close();
  }
  
  public class NodeEventCatcher extends NodeWatcher {
    private NodeEvent nodeEvent ;
    
    public void process(NodeEvent event) {
      this.nodeEvent = event ;
    }
    
    public NodeEvent getNodeEvent() { return this.nodeEvent ; }
  }
  
  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
}
