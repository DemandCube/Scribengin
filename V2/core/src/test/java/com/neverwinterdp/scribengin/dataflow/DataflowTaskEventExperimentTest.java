package com.neverwinterdp.scribengin.dataflow;


import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowTaskEventExperimentTest {
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

    ScribenginClient scribenginClient = shell.getScribenginClient();
    Assert.assertEquals(2, scribenginClient.getScribenginMasters().size());

    DataflowClient dflClient = scribenginClient.getDataflowClient("kafka-to-kafka");
    Assert.assertEquals("kafka-to-kafka-master-1", dflClient.getDataflowMaster().getId());
    Assert.assertEquals(1, dflClient.getDataflowMasters().size());

    List<VMDescriptor> dataflowWorkers = dflClient.getDataflowWorkers();
    Assert.assertEquals(2, dataflowWorkers.size());

    
    WaitingOrderNodeEventListener stopWaitingListener = new WaitingOrderNodeEventListener(scribenginClient.getRegistry());
    stopWaitingListener.add("/scribengin/dataflows/running/kafka-to-kafka/status", DataflowLifecycleStatus.PAUSE);
    dflClient.setDataflowTaskEvent(DataflowEvent.PAUSE);
    stopWaitingListener.waitForEvents(15000);
    if(stopWaitingListener.getUndetectNodeEventCount() > 0) {
      shell.execute("registry dump");
      return;
    }
    dflClient.setDataflowTaskEvent(DataflowEvent.RESUME);
    submitter.waitForTermination(90000);
  }
  
  public class DataflowSubmitter extends Thread {
    public void run() {
      try {
        String command = 
          "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          "  --dataflow-name  kafka-to-kafka" +
          "  --worker 2 --executor-per-worker 2 --duration 90000 --task-max-execute-time 1000" +
          "  --source-name input --source-num-of-stream 10 --source-write-period 5 --source-max-records-per-stream 10000" + 
          "  --sink-name output "+
          "  --debug-dataflow-activity" +
          "  --debug-dataflow-task" +
          "  --dump-registry" +
          "  --print-dataflow-info -1" ;
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
