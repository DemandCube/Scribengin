package com.neverwinterdp.scribengin.master;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.client.shell.Shell;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.registry.election.LeaderElection;
import com.neverwinterdp.scribengin.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.registry.zk.ZKRegistryFactory;
import com.neverwinterdp.scribengin.vmresource.jvm.JVMVMResourceFactory;
import com.neverwinterdp.util.FileUtil;

public class MasterElectionUnitTest {
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

  private Registry newRegistry() {
    return new RegistryImpl("127.0.0.1:2181", "/scribengin/v2");
  }
  
  private Master newScribenginMaster() throws Exception {
    String[] args = {
        "--registry-factory", ZKRegistryFactory.class.getName(), 
        "--registry-connect", "127.0.0.1:2181",
        "--registry-db-domain", "/scribengin/v2",
        
        "--vm-resource-factory", JVMVMResourceFactory.class.getName(),
      } ;
    return new Master(args) ;
  }
  
  @Test
  public void testElection() throws Exception {
    ScribenginMasterRunner[] runner = new ScribenginMasterRunner[5];
    ExecutorService executorPool = Executors.newFixedThreadPool(runner.length);
    for(int i = 0; i < runner.length; i++) {
      runner[i] = new ScribenginMasterRunner() ;
      executorPool.execute(runner[i]);
    }
    Shell shell = new Shell(newRegistry().connect()) ;
    executorPool.shutdown();
    for(int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      shell.execute("master list");
    }
    executorPool.awaitTermination(3 * 60 * 1000, TimeUnit.MILLISECONDS);
  }
  
  public class ScribenginMasterRunner implements Runnable {
    Master master;
    
    ScribenginMasterRunner() throws Exception {
      master = newScribenginMaster();
      master.start();
    }
    
    public void run() {
      try {
        for(int i = 0; i < 10; i++) {
          Thread.sleep(new Random().nextInt(3000));
          LeaderElection election = master.getLeaderElection();
          if(election != null && election.isElected()) {
            master.stop();
            master.start();
            return;
          }
        }
      } catch (Throwable  e) {
        e.printStackTrace();
      }
    }
  }
}
