package com.neverwinterdp.scribengin.client.shell;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class HelpCommandUnitTest {

  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  VMClusterBuilder vmCluster;
  ScribenginShell shell;
  VMClient vmClient;

  @Before
  public void setup() throws Exception {
    vmCluster = new EmbededVMClusterBuilder();
    vmCluster.clean();
  }

  @After
  public void teardown() throws Exception {
    vmCluster.clean();
  }

  @Test
  public void testCommands() throws Exception {
    Registry registry = new RegistryImpl();
    vmClient = new VMClient(registry);
    shell = new ScribenginShell(vmClient);
    shell.execute("help registry");
    shell.execute("help scribengin");
    shell.execute("help dummy");
    shell.execute("help");

  }

  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    return new EmbededVMClusterBuilder();
  }
}
