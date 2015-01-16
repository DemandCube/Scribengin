package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.neverwinterdp.jetty.JettyServer;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.junit.VMAssert;

public class CommandProxyServerUnitTest extends CommandProxyServletUnitTest{
  static CommandProxyServer cps;
  
  @BeforeClass
  public static void setup() throws Exception{
  //Bring up ZK and all that jazz
    testHelper = new VMUnitTest();
    testHelper.setup();

    //Launch a single VM
    shell = testHelper.newShell();
    VMAssert vmAssert = new VMAssert(shell.getVMClient());
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING, true);
    //VM vmMaster1 = createVMMaster("vm-master-1");
    CommandServletUnitTest.createVMMaster("vm-master-1");
    vmAssert.waitForEvents(5000);
    
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    Registry registry = new RegistryImpl(RegistryConfig.getDefault());
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    registry.create("/vm/commandServer", ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext commandApp = new WebAppContext();
    commandApp.setResourceBase("./src/test/resources/commandServer");
    commandApp.setDescriptor(  "./src/test/resources/commandServer/override-web.xml");
    
    
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase("./src/test/resources/commandProxyServer");
    proxyApp.setDescriptor("./src/test/resources/commandProxyServer/override-web.xml");
    
    //Bring up proxy
    cps = new CommandProxyServer(proxyPort);
    cps.setHandler(proxyApp);
    cps.startServer();
    
    //Bring up commandServer using that context
    commandServer = new JettyServer(commandPort, CommandServlet.class);
    commandServer.setHandler(commandApp);
    commandServer.start();
  }
  
  @AfterClass
  public static void teardown() throws Exception{
    cps.stop();
    commandServer.stop();
    testHelper.teardown();
  }
}
