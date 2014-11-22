package com.neverwinterdp.scribengin.registry.election;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.NodeCreateMode;
import com.neverwinterdp.scribengin.registry.RegistryService;
import com.neverwinterdp.scribengin.registry.RegistryConfig;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.scribengin.registry.zk.RegistryServiceImpl;
import com.neverwinterdp.util.FileUtil;

public class LeaderElectionUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  final static String ELECTION_PATH = "/locks" ;
  
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
    zkServerLauncher.stop();
  }

  private RegistryService newRegistry() {
    return new RegistryServiceImpl(RegistryConfig.getDefault()) ;
  }
  
  @Test
  public void testElection() throws Exception {
    String DATA = "lock directory";
    RegistryService registry = newRegistry().connect(); 
    Node electionNode = registry.create(ELECTION_PATH, DATA.getBytes(), NodeCreateMode.PERSISTENT) ;
    registry.disconnect();
    
    Leader[] leader = new Leader[10];
    ExecutorService executorPool = Executors.newFixedThreadPool(leader.length);
    for(int i = 0; i < leader.length; i++) {
      leader[i] = new Leader("worker-" + (i + 1)) ;
      executorPool.execute(leader[i]);
      if(i % 10 == 0) Thread.sleep(new Random().nextInt(50));
    }
    executorPool.shutdown();
    executorPool.awaitTermination(3 * 60 * 1000, TimeUnit.MILLISECONDS);
  }
  
  public class Leader implements Runnable {
    String name ;
    LeaderElection election;
    
    public Leader(String name) {
      this.name = name ;
    }
    
    public void run() {
      try {
        RegistryService registry = newRegistry().connect();
        Node electionPath =  registry.get(ELECTION_PATH) ;
        election = electionPath.getLeaderElection();
        election.setListener(new LeaderElectionListener() {
          public void onElected() {
            System.out.println(name + " is elected");
          }
        });
        election.start();
        Node node = election.getNode();
        node.setData(name.getBytes());
        Assert.assertEquals(name, new String(node.getData())) ;
        int count = 0;
        while(count < 25) {
          try {
            Thread.sleep(500);
            if(election.isElected()) {
              election.stop();
              Thread.sleep(500);
              election.start();
            }
            count++ ;
          } catch(InterruptedException ex) {
            break;
          }
        }
        election.stop();
        registry.disconnect();
      } catch(RegistryException e) {
        e.printStackTrace();
      }
    }
  }
}
