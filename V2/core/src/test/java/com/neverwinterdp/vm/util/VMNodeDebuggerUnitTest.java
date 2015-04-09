package com.neverwinterdp.vm.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class VMNodeDebuggerUnitTest {
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
    
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());

   
  }

  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    Thread.sleep(5000);
  }
  
  @Test
  public void testVMNodeDebugger() throws Exception{
    shell.execute("registry dump");
    
    RegistryDebugger debugger = new RegistryDebugger(System.out, shell.getVMClient().getRegistry()) ;
    debugger.watch("/vm/allocated/vm-scribengin-master-1", new VMNodeDebugger(), true);
    
    clusterBuilder.startScribenginMasters();
    
    shell.execute("registry dump");
    
    debugger.clear();
  }

  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    return new EmbededVMClusterBuilder();
  }
}
