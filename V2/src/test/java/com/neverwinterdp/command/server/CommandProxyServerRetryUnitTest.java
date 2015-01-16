package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;

import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.junit.VMAssert;

public class CommandProxyServerRetryUnitTest extends CommandProxyServletRetryUnitTest{
  CommandProxyServer cps;
  
  @Before
  public void setup() throws Exception{
    //Bring up ZK and all that jazz
    testHelper = new VMUnitTest();
    testHelper.setup();

    //Launch a single VM
    shell = testHelper.newShell();
    VMAssert vmAssert = new VMAssert(shell.getVMClient());
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING);
    //VM vmMaster1 = createVMMaster("vm-master-1");
    CommandServletUnitTest.createVMMaster("vm-master-1");
    vmAssert.waitForEvents(5000);
    
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    registry = new RegistryImpl(RegistryConfig.getDefault());
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    registry.create(registryPath, ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase("./src/test/resources/commandProxyServer");
    proxyApp.setDescriptor("./src/test/resources/commandProxyServer/override-web.xml");
    
    //Bring up proxy
    cps = new CommandProxyServer(proxyPort);
    cps.setHandler(proxyApp);
    cps.startServer();
  }
  
  @After
  public void teardown() throws Exception{
    System.out.println("Stopping servers");
    cps.stop();
    testHelper.teardown();
  }
}
