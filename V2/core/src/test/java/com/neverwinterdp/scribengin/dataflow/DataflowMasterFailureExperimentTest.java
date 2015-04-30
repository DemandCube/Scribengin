package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig.FailurePoint;
import com.neverwinterdp.scribengin.dataflow.test.DataflowRandomServerFailureTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;

public class DataflowMasterFailureExperimentTest {
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
    DataflowClient dflClient = scribenginClient.getDataflowClient("kafka-to-kafka");
    FailureConfig failureConfig = new FailureConfig("run-dataflow", "create-dataflow-worker", FailurePoint.After) ;
    dflClient.getDataflowRegistry().broadcastFailureEvent(failureConfig);
    submitter.waitForTermination(180000);
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
          "  --debug-dataflow-activity" +
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
