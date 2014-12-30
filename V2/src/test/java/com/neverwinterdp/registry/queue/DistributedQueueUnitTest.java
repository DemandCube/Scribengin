package com.neverwinterdp.registry.queue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class DistributedQueueUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  private ZookeeperServerLauncher zkServerLauncher ;
  private Registry registry;
  private DistributedQueue queue;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
    registry = new RegistryImpl(RegistryConfig.getDefault()).connect() ;
    queue = new DistributedQueue(registry, "/queue") ;
  }
  
  @After
  public void teardown() throws Exception {
    registry.disconnect();
    zkServerLauncher.shutdown();
  }
  
  @Test
  public void testOffer() throws Exception {
    offer(10);
  }

  @Test
  public void testPoll() throws Exception {
    offer(10);
    byte[] data = null ;
    int count = 0;
    while((data = queue.poll()) != null) {
      Assert.assertEquals("hello " + count++, new String(data));
    }
  }

  
  @Test
  public void testTake() throws Exception {
    int size = 10;
    offer(size);
    Thread thread = new Thread() {
      int count = 0;
      
      public void run() {
        try {
          while(true) {
            byte[] data = queue.take() ;
            Assert.assertEquals("hello " + count++, new String(data));
          }
        } catch (RegistryException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
    thread.start();
    Thread.sleep(1000);
    thread.interrupt();
  }
  
  void offer(int size) throws Exception {
    for(int i = 0 ; i < size; i++) {
      String data = "hello " + i ;
      queue.offer(data.getBytes());
    }
  }
}
