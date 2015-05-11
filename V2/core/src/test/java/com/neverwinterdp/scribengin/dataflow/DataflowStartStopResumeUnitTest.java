package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.DataflowCommandStartStopResumeTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;

public class DataflowStartStopResumeUnitTest {
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
    DataflowSubmitter submitter = new DataflowSubmitter();
    submitter.start();

    ScribenginClient scribenginClient = shell.getScribenginClient();
    DataflowClient dflClient = scribenginClient.getDataflowClient("kafka-to-kafka-1");
    Assert.assertEquals("kafka-to-kafka-1-master-1", dflClient.getDataflowMaster().getId());
    Assert.assertEquals(1, dflClient.getDataflowMasters().size());

    StartStopResumeRunner startStopResumeRunner = new StartStopResumeRunner() ;
    startStopResumeRunner.start();
    
    submitter.waitForTermination(180000);
    startStopResumeRunner.waitForTermination(30000);
  }
  
  public class StartStopResumeRunner extends Thread {
    boolean running ;
    public void run() {
      running = true;
      try {
        String command = 
            "dataflow-test " + DataflowCommandStartStopResumeTest.TEST_NAME +
            "  --dataflow-id kafka-to-kafka-1" +
            "  --dataflow-name kafka-to-kafka" +
            "  --sleep-before-stop 10000 --sleep-before-resume 5000" +
            "  --max-wait-for-stop    10000 " +
            "  --max-wait-for-resume  5000 " +
            "  --max-execution 3" +
            "  --print-summary ";
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
          "  --dataflow-id kafka-to-kafka-1" +
          "  --dataflow-name  kafka-to-kafka" +
          "  --worker 2 --executor-per-worker 2 --duration 180000 --task-max-execute-time 10000" +
          "  --source-name input --source-num-of-stream 10 --source-write-period 0 --source-max-records-per-stream 100000" + 
          "  --sink-name output "+
          "  --debug-dataflow-activity-detail" +
          "  --debug-dataflow-task" +
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
