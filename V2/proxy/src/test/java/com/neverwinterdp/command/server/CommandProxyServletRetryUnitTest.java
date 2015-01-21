package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.neverwinterdp.jetty.JettyServer;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.junit.VMAssert;

public class CommandProxyServletRetryUnitTest {
  protected static JettyServer proxyServer;
  protected int commandPort = 8181;
  protected int commandPort2 = 8182;
  protected int proxyPort = 8383;
  Registry registry;
  String registryPath = "/vm/commandServer";
  String expectedListVMResponse = 
                    "Running VM\n"+
                    "-----------------------------------------------------------------------\n"+
                    "ID            Path                        Roles       Cores   Memory   \n"+
                    "-----------------------------------------------------------------------\n"+
                    "vm-master-1   /vm/allocated/vm-master-1   vm-master   1       128      \n\n";
  
  //Used to bring up VMs to test with
  CommandServerTestHelper testHelper;
  Shell shell;
  VMClient vmClient;
  
  @Before
  public void setup() throws Exception{
    //Bring up ZK and all that jazz
    testHelper = new CommandServerTestHelper();
    testHelper.assertWebXmlFilesExist();
    testHelper.setup();

    registry = new RegistryImpl(RegistryConfig.getDefault());
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    //Launch a single VM
    shell = testHelper.newShell();
    VMAssert vmAssert = new VMAssert(registry);
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING);
    //VM vmMaster1 = createVMMaster("vm-master-1");
    CommandServletUnitTest.createVMMaster("vm-master-1");
    vmAssert.waitForEvents(5000);
    
    
    
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    registry.create(registryPath, ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    //Start proxy
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase(testHelper.getProxyServerFolder());
    proxyApp.setDescriptor(  testHelper.getProxyServerXml());
    
    proxyServer = new JettyServer(proxyPort, CommandProxyServlet.class);
    proxyServer.setHandler(proxyApp);
    proxyServer.start();
  }

  
  @After
  public void teardown() throws Exception{
    System.out.println("Stopping servers");
    proxyServer.stop();
    testHelper.teardown();
  }
  
  @Test
  public void testCommandServletListVMs() throws Exception{
    Unirest.setTimeouts(1000L, 1000L);
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext commandApp = new WebAppContext();
    commandApp.setResourceBase(testHelper.getCommandServerFolder());
    commandApp.setDescriptor(testHelper.getCommandServerXml());
    
    //Bring up commandServer using that context
    JettyServer commandServer = new JettyServer(commandPort, CommandServlet.class);
    commandServer.setHandler(commandApp);
    commandServer.start();
    
    //Test the proxy is working to begin with
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
           .field("command", "vm list")
           .asString();
    
    assertEquals(expectedListVMResponse, resp.getBody());
    
    //Kill the original command server
    commandServer.stop();
    
    //Set the localation for where the new command server will be
    //Running on a different port
    registry.setData(registryPath, ("http://localhost:"+Integer.toString(commandPort2)).getBytes());
    
    //Bring up the new command server on the 2nd port
    JettyServer commandServer2 = new JettyServer(commandPort2, CommandServlet.class);
    commandServer2.setHandler(commandApp);
    commandServer2.start();
    
    //System.err.println("Joining");
    //proxyServer.join();
    
    
    //Query the proxy again, it should fix itself and return the expected String
    HttpResponse<String> resp2 = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
        .field("command", "vm list")
        .asString();
    assertEquals(expectedListVMResponse, resp2.getBody());
    commandServer2.stop();
  }
  
}
