package com.neverwinterdp.scribengin;


import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowUnitTest {
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
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }
  
  @Test
  public void testDataflows() throws Exception {
    DataflowSubmitter submitter = new DataflowSubmitter();
    submitter.start();

    Thread.sleep(15000); //make sure that the dataflow start and running;

    try {
      ScribenginClient scribenginClient = shell.getScribenginClient();
      Assert.assertEquals(2, scribenginClient.getScribenginMasters().size());

      DataflowClient dataflowClient = scribenginClient.getDataflowClient("hello-kafka-dataflow");
      Assert.assertEquals("hello-kafka-dataflow-master-1", dataflowClient.getDataflowMaster().getId());
      Assert.assertEquals(1, dataflowClient.getDataflowMasters().size());
      
      VMClient vmClient = scribenginClient.getVMClient();
      List<VMDescriptor> dataflowWorkers = dataflowClient.getDataflowWorkers();
      Assert.assertEquals(2, dataflowWorkers.size());
      vmClient.shutdown(dataflowWorkers.get(1));
      Thread.sleep(2000);
      shell.execute("registry   dump");
      submitter.waitForTermination(300000);
      
      Thread.sleep(3000);
      shell.execute("vm         info");
      shell.execute("scribengin info");
      shell.execute("dataflow   info --history hello-kafka-dataflow-0");
      shell.execute("registry   dump");
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
          "dataflow-test kafka " + 
          "  --worker 2 --executor-per-worker 2 --duration 70000 --task-max-execute-time 1000" +
          "  --kafka-num-partition 10 --kafka-write-period 5 --kafka-max-message-per-partition 3000";
        shell.execute(command);
      } catch(Exception ex) {
        ex.printStackTrace();
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