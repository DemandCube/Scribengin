package com.neverwinterdp.scribengin.client.shell;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class HelpCommandUnitTest {

  static {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  EmbededVMClusterBuilder vmCluster;
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
  }

  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }

}
