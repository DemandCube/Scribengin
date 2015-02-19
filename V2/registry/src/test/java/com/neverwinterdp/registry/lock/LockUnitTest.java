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

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.lock.LockId;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
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
    Worker[] worker = new Worker[50];
    ExecutorService executorPool = Executors.newFixedThreadPool(worker.length);
    for(int i = 0; i < worker.length; i++) {
      worker[i] = new Worker("worker-" + (i + 1)) ;
      executorPool.execute(worker[i]);
      if(i % 10 == 0) Thread.sleep(new Random().nextInt(50));
    }
    executorPool.shutdown();
    executorPool.awaitTermination(15 * 60 * 1000, TimeUnit.MILLISECONDS);
    for(int i = 0; i < worker.length; i++) {
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
        lockId = lock.lock(60 * 1000) ; //wait max 15s for lock
        System.out.println("\nWorker " + name + " acquires the lock: " + lockId);
        long execTime = random.nextInt(1000) ;
        Thread.sleep(execTime);
        System.out.println(" Process in " + execTime);
        Assert.assertEquals(lockOrder.getAndIncrement(), lockId.getSequence()) ;
        lock.unlock();
        System.out.println("Worker " + name + " releases the lock: " + lockId);
        complete = true ;
        registry.disconnect();
      } catch(RegistryException e) {
        if(e.getErrorCode() == ErrorCode.Timeout) {
          complete = true ;
          System.err.println(e.getMessage()) ;
        } else {
          e.printStackTrace();
        }
      } catch(Exception e) {
        e.printStackTrace();
      } 
    }
  }
}
