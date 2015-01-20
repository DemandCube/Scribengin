package com.neverwinterdp.registry.lock;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import com.neverwinterdp.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.lock.LockId;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;

public class LockUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  final static String LOCK_DIR = "/locks" ;
  
  private ZookeeperServerLauncher zkServerLauncher ;
  private AtomicLong lockOrder ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    lockOrder = new AtomicLong() ;
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    zkServerLauncher.shutdown();
  }

  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
  
  @Test
  public void testConcurrentLock() throws Exception {
    String DATA = "lock directory";
    Registry registry = newRegistry().connect(); 
    Node lockDir = registry.create(LOCK_DIR, DATA.getBytes(), NodeCreateMode.PERSISTENT) ;
    registry.disconnect();
    Worker[] worker = new Worker[100];
    ExecutorService executorPool = Executors.newFixedThreadPool(worker.length);
    for(int i = 0; i < worker.length; i++) {
      worker[i] = new Worker("worker-" + (i + 1)) ;
      executorPool.execute(worker[i]);
      if(i % 10 == 0) Thread.sleep(new Random().nextInt(50));
    }
    executorPool.shutdown();
    executorPool.awaitTermination(5 * 60 * 1000, TimeUnit.MILLISECONDS);
    for(int i = 0; i < worker.length; i++) {
      Assert.assertNotNull(worker[i].lockId);
      Assert.assertTrue(worker[i].complete);
    }
  }
  
  public class Worker implements Runnable {
    String name ;
    LockId lockId ;
    boolean complete = false;
    
    public Worker(String name) {
      this.name = name ;
    }
    
    public void run() {
      try {
        Random random = new Random() ;
        Thread.sleep(random.nextInt(100));
        Registry registry = newRegistry().connect();
        Node lockDir =  registry.get(LOCK_DIR) ;
        Lock lock = lockDir.getLock("write") ;
        lockId = lock.lock(3 * 60 * 1000) ; //wait max 3 min for lock
        System.out.println("\nWorker " + name + " acquires the lock: " + lockId);
        long execTime = random.nextInt(100) ;
        Thread.sleep(execTime);
        System.out.println(" Process in " + execTime);
        Assert.assertEquals(lockOrder.getAndIncrement(), lockId.getSequence()) ;
        lock.unlock();
        System.out.println("ScribenginMasterRunner " + name + " releases the lock: " + lockId);
        registry.disconnect();
        complete = true ;
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}
