package com.neverwinterdp.scribengin;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

public class ScribenginCommandShutdownUnitTest {
  
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected ScribenginClusterBuilder clusterBuilder;
  protected ScribenginShell          shell;

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
  }
  
  @Test
  public void testMasterShutdownCommand() throws Exception {

    ScribenginClient scribenginClient = shell.getScribenginClient();
    assertEquals(2, scribenginClient.getScribenginMasters().size());

    shell.execute("registry  dump");
    shell.execute("scribengin master  --shutdown");
    assertEquals(1, scribenginClient.getScribenginMasters().size());

    shell.execute("registry   dump");
  }
  
  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }

}
