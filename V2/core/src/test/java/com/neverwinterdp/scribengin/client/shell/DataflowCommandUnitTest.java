package com.neverwinterdp.scribengin.client.shell;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

public class DataflowCommandUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected static ScribenginClusterBuilder clusterBuilder;
  protected static ScribenginShell shell;

  @Before
  public void setup() throws Exception {
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
    Thread.sleep(15000);
  }

  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }

  @Test
  public void testMasterListCommand() throws Exception {
    //TODO: what is this test for
    DataflowSubmitter submitter = new DataflowSubmitter();
    submitter.start();

    Thread.sleep(15000); // make sure that the dataflow start and running;
    shell.execute("registry dump");
    // shell.execute("dataflow master --list");
  }

  @Test
  @Ignore
  public void testMasterShutdownCommand() throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient();
    assertEquals(2, scribenginClient.getScribenginMasters().size());
    //TODO: The dataflow shutdown command should look like:
    // dataflow shutdown --dataflow-id=id
    // shell.execute("registry  dump");
    shell.execute("dataflow master  --shutdown");
    assertEquals(1, scribenginClient.getScribenginMasters().size());

    // shell.execute("registry  dump");
  }

  @Test
  @Ignore
  public void testMasterInvalidCommand() throws Exception {

    ScribenginClient scribenginClient = shell.getScribenginClient();
    assertEquals(2, scribenginClient.getScribenginMasters().size());

    // shell.execute("registry   dump");
    shell.execute("dataflow master  --dummy");
  }

  public class DataflowSubmitter extends Thread {
    public void run() {
      try {
        String command =
            "dataflow-test kafka "
                +
                "  --worker 3 --executor-per-worker 1 --duration 70000 --task-max-execute-time 1000"
                +
                "  --kafka-num-partition 10 --kafka-write-period 5 --kafka-max-message-per-partition 3000";
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
