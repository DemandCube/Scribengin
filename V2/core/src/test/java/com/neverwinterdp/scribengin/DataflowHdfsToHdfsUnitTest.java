package com.neverwinterdp.scribengin;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class DataflowHdfsToHdfsUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected ScribenginClusterBuilder clusterBuilder;
  protected ScribenginShell shell;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/storage", false);
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder());
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
    Thread.sleep(5000); // make sure that the dataflow start and running;

    try {
      ScribenginClient scribenginClient = shell.getScribenginClient();
      Assert.assertEquals(2, scribenginClient.getScribenginMasters().size());

//      shell.execute("registry dump");
//      DataflowClient dflClient = scribenginClient.getDataflowClient("hello-hdfs-dataflow");
//      Assert.assertEquals("hello-hdfs-dataflow-master-1", dflClient.getDataflowMaster().getId());
//      Assert.assertEquals(1, dflClient.getDataflowMasters().size());
//
//      VMClient vmClient = scribenginClient.getVMClient();
//      List<VMDescriptor> dataflowWorkers = dflClient.getDataflowWorkers();
//      Assert.assertEquals(3, dataflowWorkers.size());
//      vmClient.shutdown(dataflowWorkers.get(1));
//      Thread.sleep(2000);
      shell.execute("registry   dump");

      Thread.sleep(3000);
      shell.execute("vm         info");
      shell.execute("scribengin info");
      shell.execute("dataflow   info --history hello-hdfs-dataflow-0");
    } catch (Throwable err) {
      throw err;
    } finally {
      if (submitter.isAlive())
        submitter.interrupt();
    }
  }

  public class DataflowSubmitter extends Thread {
    public void run() {
      try {
        String command =
            "dataflow-test hdfs " +
                "  --worker 3 " + 
                " --executor-per-worker 1 " + 
                " --duration 10000" + 
                " --task-max-execute-time 1000" +
                " --source-num-of-stream 10" +
                " --source-max-records-per-stream 1000" +
                " --source-name hello-source" +
                " --sink-name hello-sink";
        shell.execute(command);
      } catch (Exception ex) {
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