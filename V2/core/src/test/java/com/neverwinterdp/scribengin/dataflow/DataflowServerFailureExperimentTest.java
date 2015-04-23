package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.DataflowRandomServerFailureTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;

public class DataflowServerFailureExperimentTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  protected ScribenginClusterBuilder clusterBuilder ;
  protected ScribenginShell shell;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void testDataflows() throws Exception {
    ServerFailureSimulator serverFailureSimulator = new ServerFailureSimulator() ;
    serverFailureSimulator.start();
    
    DataflowSubmitter submitter = new DataflowSubmitter();
    submitter.start();
    
    Thread.sleep(10000);
    
    ScribenginClient scribenginClient = shell.getScribenginClient();
    Assert.assertEquals(2, scribenginClient.getScribenginMasters().size());
    
    DataflowClient dflClient = scribenginClient.getDataflowClient("kafka-to-kafka");
    Assert.assertEquals("kafka-to-kafka-master-1", dflClient.getDataflowMaster().getId());
    Assert.assertEquals(1, dflClient.getDataflowMasters().size());

    submitter.waitForTermination(180000);
    serverFailureSimulator.waitForTermination(30000);
  }
  
  public class ServerFailureSimulator extends Thread {
    boolean running ;
    public void run() {
      running = true;
      try {
        String command = 
          "dataflow-test " + DataflowRandomServerFailureTest.TEST_NAME + 
          "  --dataflow-name kafka-to-kafka" + 
          "  --failure-period 10000 --max-failure 2 --simulate-kill";
        shell.execute(command);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        notifyTermimation();
        running = false;
      }
    }
    
    synchronized void notifyTermimation() {
      notify();
    }
    
    synchronized void waitForTermination(long timeout) throws InterruptedException {
      if(!running) return ;
      wait(timeout);
    }
  }
  
  public class DataflowSubmitter extends Thread {
    private boolean running = false;
    public void run() {
      running = true;
      try {
        String command = 
          "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          "  --dataflow-name  kafka-to-kafka" +
          "  --worker 2 --executor-per-worker 2 --duration 180000 --task-max-execute-time 5000" +
          "  --source-name input --source-num-of-stream 10 --source-write-period 0 --source-max-records-per-stream 100000" + 
          "  --sink-name output "+
          "  --debug-dataflow-activity-detail" +
          "  --debug-dataflow-task" +
          "  --debug-dataflow-vm" +
          "  --dump-registry" +
          "  --print-dataflow-info -1" ;
        shell.execute(command);
      } catch(Exception ex) {
        ex.printStackTrace();
      } finally {
        notifyTermimation();
        running = false;
      }
    }
    
    synchronized void notifyTermimation() {
      notify();
    }
    
    synchronized void waitForTermination(long timeout) throws InterruptedException {
      if(!running) return ;
      wait(timeout);
    }
  }
}
