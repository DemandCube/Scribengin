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

    int stopCount = 0; 
    int stopCompleteCount = 0; 
    int resumeCount = 0 ;
    int resumeCompleteCount = 0; 
    while(stopCount < 10) {
      long startStopTime =  System.currentTimeMillis();
      stopCount++ ;
      DataflowEvent event = DataflowEvent.PAUSE ;
      DataflowLifecycleStatus expectStatus = DataflowLifecycleStatus.PAUSE;
      if(stopCount % 2 == 0) {
        event = DataflowEvent.STOP ;
        expectStatus = DataflowLifecycleStatus.STOP;
      }
      WaitingOrderNodeEventListener stopWaitingListener = new WaitingOrderNodeEventListener(dflClient.getRegistry());
      stopWaitingListener.add(dflClient.getDataflowRegistry().getStatusNode().getPath(), expectStatus);
      dflClient.setDataflowEvent(event);
      stopWaitingListener.waitForEvents(20000);
      if(stopWaitingListener.getUndetectNodeEventCount() > 0) {
        break;
      }
      stopCompleteCount++ ;
      System.err.println("Stop in " + (System.currentTimeMillis() - startStopTime) + "ms");
      
      
      long startResumeTime =  System.currentTimeMillis();
      WaitingOrderNodeEventListener resumeWaitingListener = new WaitingOrderNodeEventListener(dflClient.getRegistry());
      resumeWaitingListener.add(dflClient.getDataflowRegistry().getStatusNode().getPath(), DataflowLifecycleStatus.RUNNING);
      dflClient.setDataflowEvent(DataflowEvent.RESUME);
      resumeCount++ ;
      resumeWaitingListener.waitForEvents(20000);
      if(resumeWaitingListener.getUndetectNodeEventCount() > 0) {
        break;
      }
      resumeCompleteCount++ ;
      System.err.println("Resume in " + (System.currentTimeMillis() - startResumeTime) + "ms");
    }
    submitter.waitForTermination(300000);
    System.out.println("stop count  = " + stopCount + ", stop complete count = " + stopCompleteCount);
    System.out.println("resume count  = " + resumeCount + ", resume complete count = " + resumeCompleteCount);
  }
  
  
  
  public class DataflowSubmitter extends Thread {
    private boolean running = false;
    public void run() {
      running = true;
      try {
        String command = 
          "dataflow-test " + KafkaDataflowTest.TEST_NAME +
          "  --dataflow-name  kafka-to-kafka" +
          "  --worker 2 --executor-per-worker 2 --duration 300000 --task-max-execute-time 5000" +
          "  --source-name input --source-num-of-stream 10 --source-write-period 0 --source-max-records-per-stream 200000" + 
          "  --sink-name output "+
          "  --debug-dataflow-activity" +
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
