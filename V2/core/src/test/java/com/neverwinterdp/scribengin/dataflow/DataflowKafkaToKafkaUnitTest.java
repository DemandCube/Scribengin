package com.neverwinterdp.scribengin.dataflow;


import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowKafkaToKafkaUnitTest {
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

    Thread.sleep(5000); //make sure that the dataflow start and running;

    try {
      ScribenginClient scribenginClient = shell.getScribenginClient();
      assertEquals(2, scribenginClient.getScribenginMasters().size());
      
      DataflowClient dataflowClient = scribenginClient.getDataflowClient("kafka-to-kafka");
      assertEquals("kafka-to-kafka-master-1", dataflowClient.getDataflowMaster().getId());
      assertEquals(1, dataflowClient.getDataflowMasters().size());
      
      VMClient vmClient = scribenginClient.getVMClient();
      List<VMDescriptor> dataflowWorkers = dataflowClient.getDataflowWorkers();
      assertEquals(3, dataflowWorkers.size());
      vmClient.shutdown(dataflowWorkers.get(0));
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
            " --source-name input" +
            " --source-num-of-stream 10" +
            " --source-write-period 5" +
            " --source-max-records-per-stream 10000" +
            " --sink-name output " +
            " --print-dataflow-info -1" +
            " --debug-dataflow-task " +
            " --debug-dataflow-vm " +
            " --debug-dataflow-activity " +
            //" --debug-dataflow-task-detail " +
            //" --debug-dataflow-vm-detail " +
            //" --debug-dataflow-activity-detail " +
            " --junit-report build/junit-report.xml" + 
            " --dump-registry";
        shell.execute(command);
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