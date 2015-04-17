package com.neverwinterdp.scribengin.dataflow;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.HDFSDataflowTest;
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
      assertEquals(2, scribenginClient.getScribenginMasters().size());

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
            "dataflow-test " + HDFSDataflowTest.TEST_NAME +
                " --dataflow-name  hdfs-to-hdfs" +
                " --worker 3" +
                " --executor-per-worker 1" +
                " --duration 90000" +
                " --task-max-execute-time 1000" +
                " --source-name output" +
                " --source-num-of-stream 10" +
                " --source-write-period 5" +
                " --source-max-records-per-stream 1000" +
                " --sink-name output" +
                " --print-dataflow-info -1" +
                " --debug-dataflow-task " +
                " --debug-dataflow-vm " +
                " --debug-dataflow-activity " +
                " --junit-report build/junit-report.xml" +
                " --dump-registry";
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