package com.neverwinterdp.scribengin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.Formater;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class ScribenginCommandUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected static ScribenginClusterBuilder clusterBuilder;
  protected static ScribenginShell          shell;

  @BeforeClass
  public static void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
    Thread.sleep(3000);
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }

  @AfterClass
  public static void teardown() throws Exception {
    clusterBuilder.shutdown();
  }

  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    return new EmbededVMClusterBuilder();
  }

  @Test
  public void testMasterListCommand() throws Exception {

    ScribenginClient scribenginClient = shell.getScribenginClient();
    assertEquals(2, scribenginClient.getScribenginMasters().size());

    shell.execute("registry dump");
    shell.execute("scribengin master --list");
    List<VMDescriptor> descriptors = scribenginClient.getScribenginMasters();

    Formater.VmList formater = new Formater.VmList(descriptors,"/vm/allocated/vm-scribengin-master-2");
    String formattedText = formater.format("Masters");
    assertTrue(formattedText.contains("vm-scribengin-master-1"));
    assertTrue(formattedText.contains("/vm/allocated/vm-scribengin-master-2"));
  }

  @Test
  public void testMasterInvalidCommand() throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient();
    assertEquals(2, scribenginClient.getScribenginMasters().size());

    // shell.execute("registry   dump");
    shell.execute("scribengin master  --dummy");
  }
}
