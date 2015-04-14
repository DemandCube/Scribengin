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
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class DataflowTaskEventExperimentTest {
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
      
      shell.execute("registry   dump");
      
      DataflowClient dflClient = scribenginClient.getDataflowClient("kafka-to-kafka");
      Assert.assertEquals("kafka-to-kafka-master-1", dflClient.getDataflowMaster().getId());
      Assert.assertEquals(1, dflClient.getDataflowMasters().size());
      
      List<VMDescriptor> dataflowWorkers = dflClient.getDataflowWorkers();
      Assert.assertEquals(2, dataflowWorkers.size());
      
      dflClient.setDataflowTaskEvent(DataflowEvent.STOP);
      Thread.sleep(5000);
      shell.execute("registry dump");
      dflClient.setDataflowTaskEvent(DataflowEvent.RESUME);
      Thread.sleep(5000);
      shell.execute("registry dump");
      submitter.waitForTermination(60000);
      
      Thread.sleep(3000);
      shell.execute("vm info");
    } catch(Throwable err) {
      throw err;
    } finally {
      shell.execute("registry dump");
      if(submitter.isAlive()) submitter.interrupt();
    }
  }
  
  public class DataflowSubmitter extends Thread {
    public void run() {
      try {
        String command = 
            "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          " --dataflow-name  kafka-to-kafka" +
          "  --worker 2 --executor-per-worker 2 --duration 90000 --task-max-execute-time 1000" +
          "  --source-name input --source-num-of-stream 10 --source-write-period 5 --source-max-records-per-stream 10000" + 
          "  --sink-name output "+
          "  --junit-report build/tap.xml";
        shell.execute(command);
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
    
    synchronized void waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
    }
  }
}
