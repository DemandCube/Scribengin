package com.neverwinterdp.scribengin.master;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.scribengin.ScribenginUnitTest;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

public class MasterElectionUnitTest extends ScribenginUnitTest {
  private Master newScribenginMaster() throws Exception {
    return newMasterContainer().getInstance(Master.class) ;
  }
  
  @Test
  public void testElection() throws Exception {
    ScribenginMasterRunner[] runner = new ScribenginMasterRunner[5];
    ExecutorService executorPool = Executors.newFixedThreadPool(runner.length);
    for(int i = 0; i < runner.length; i++) {
      runner[i] = new ScribenginMasterRunner() ;
      executorPool.execute(runner[i]);
    }
    ScribenginShell shell = new ScribenginShell(newRegistry().connect()) ;
    executorPool.shutdown();
    for(int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      shell.execute("scribengin master");
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
