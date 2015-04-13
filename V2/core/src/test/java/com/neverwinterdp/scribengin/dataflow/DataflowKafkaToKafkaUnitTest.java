package com.neverwinterdp.scribengin.dataflow;


import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class DataflowKafkaToKafkaUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  protected ScribenginClusterBuilder clusterBuilder ;
  protected ScribenginShell shell;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    Thread.sleep(3000);
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    return new EmbededVMClusterBuilder();
  }
  
  @Test
  public void testDataflows() throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter();
    submitter.start();

    Thread.sleep(5000); //make sure that the dataflow start and running;

    try {
      ScribenginClient scribenginClient = shell.getScribenginClient();
      Assert.assertEquals(2, scribenginClient.getScribenginMasters().size());
      
      DataflowClient dataflowClient = scribenginClient.getDataflowClient("kafka-to-kafka");
      Assert.assertEquals("kafka-to-kafka-master-1", dataflowClient.getDataflowMaster().getId());
      Assert.assertEquals(1, dataflowClient.getDataflowMasters().size());
      
      VMClient vmClient = scribenginClient.getVMClient();
      List<VMDescriptor> dataflowWorkers = dataflowClient.getDataflowWorkers();
      Assert.assertEquals(3, dataflowWorkers.size());
      vmClient.shutdown(dataflowWorkers.get(1));
      submitter.waitForTermination(300000);
    } catch(Throwable err) {
      throw err;
    } finally {
      if(submitter.isAlive()) submitter.interrupt();
    }
  }
  
  public class DataflowSubmitter extends Thread {
    public void run() {
      try {
        String command =
            "dataflow-test " + KafkaDataflowTest.TEST_NAME +
            " --dataflow-name  kafka-to-kafka" +
            " --worker 3" +
            " --executor-per-worker 1" +
            " --duration 90000" +
            " --task-max-execute-time 1000" +
            "  --source-dataflowName input" +
            " --source-num-of-stream 10" +
            " --source-write-period 5" +
            " --source-max-records-per-stream 10000" +
            " --sink-dataflowName output " +
            " --print-dataflow-info -1" +
            " --debug-dataflow-task true" +
            " --debug-dataflow-worker true" +
            " --junit-report build/junit-report.xml";
        shell.execute(command);
        shell.execute("registry dump");
      } catch(Exception ex) {
        ex.printStackTrace();
      } finally {
        notifyTermimation() ;
      }
    }
    
    synchronized void notifyTermimation() {
      notify();
    }
    
    synchronized void waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
    }
  }
}