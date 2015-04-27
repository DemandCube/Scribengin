package com.neverwinterdp.scribengin.dataflow;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.S3ToS3DataflowTest;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

/*
 * A unit test that shouldn't run all the time
 * */

public class DataflowS3ToS3IntegrationTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected ScribenginClusterBuilder clusterBuilder;
  protected ScribenginShell shell;

  private S3Client s3Client;

  private String folderPath;
  private String bucketName;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/storage", false);
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());

    bucketName = "s3-integration-test-" + UUID.randomUUID();
    folderPath = "dataflow-test";
    
    s3Client = new S3Client();
    s3Client.onInit();

    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteS3Folder(bucketName, folderPath);
    }
    s3Client.createBucket(bucketName);
  }

  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();

    s3Client.deleteBucket(bucketName, true);
    s3Client.onDestroy();
  }

  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    return new EmbededVMClusterBuilder();
  }

  @Test
  public void testDataflows() throws Exception {
    int numStreams = 2;

    for (int i = 1; i <= numStreams; i++) {
      s3Client.createS3Folder(bucketName, folderPath + "/stream-" + i);
    }
    DataflowSubmitter submitter = new DataflowSubmitter(bucketName, folderPath, numStreams);
    submitter.start();
    Thread.sleep(5000); // make sure that the dataflow start and running;

    try {
      ScribenginClient scribenginClient = shell.getScribenginClient();
      assertEquals(2, scribenginClient.getScribenginMasters().size());

      Thread.sleep(3000);
      shell.execute("vm         info");
      shell.execute("scribengin info");
      shell.execute("dataflow   info --history hello-s3-dataflow-0");
    } catch (Throwable err) {
      throw err;
    } finally {
      if (submitter.isAlive())
        submitter.interrupt();
    }
  }

  public class DataflowSubmitter extends Thread {
    private String bucketName;
    private String folderPath;
    private int numStreams;

    public DataflowSubmitter(String bucketName, String folderPath, int numStreams) {
      this.bucketName = bucketName;
      this.folderPath = folderPath;
      this.numStreams = numStreams;
    }

    public void run() {
      try {
        String command =
            "dataflow-test " + S3ToS3DataflowTest.TEST_NAME +
                " --dataflow-name  s3-to-s3" +
                " --worker 1" +
                " --executor-per-worker 1" +
                " --duration 90000" +
                " --task-max-execute-time 100000" +
                " --source-location " + bucketName +
                " --source-name " + folderPath +
                " --source-num-of-stream " + numStreams +
                " --source-max-records-per-stream 100" +
                " --sink-location " + bucketName +
                " --sink-name " + folderPath +
                " --print-dataflow-info -1" +
                " --debug-dataflow-task true" +
                " --debug-dataflow-worker true" +
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