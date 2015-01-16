package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.neverwinterdp.jetty.JettyServer;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.junit.VMAssert;

public class CommandProxyServletUnitTest {
  protected static JettyServer commandServer;
  protected static JettyServer proxyServer;
  protected static int commandPort = 8181;
  protected static int proxyPort = 8383;
  protected String expectedListVMResponse = 
                    "Running VM\n"+
                    "-----------------------------------------------------------------------\n"+
                    "ID            Path                        Roles       Cores   Memory   \n"+
                    "-----------------------------------------------------------------------\n"+
                    "vm-master-1   /vm/allocated/vm-master-1   vm-master   1       128      \n\n";
  
  //Used to bring up VMs to test with
  static VMUnitTest testHelper;
  static Shell shell;
  static VMClient vmClient;
  
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
    
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase("./src/test/resources/commandProxyServer");
    proxyApp.setDescriptor(  "./src/test/resources/commandProxyServer/override-web.xml");
    
    proxyServer = new JettyServer(proxyPort, CommandProxyServlet.class);
    proxyServer.setHandler(proxyApp);
    proxyServer.start();
    
    
    //Bring up commandServer using that context
    commandServer = new JettyServer(commandPort, CommandServlet.class);
    commandServer.setHandler(commandApp);
    commandServer.start();
    
    //Uncomment this line to make the proxyServer run indefinitely
    //proxyServer.join();
    
  }

  
  @AfterClass
  public static void teardown() throws Exception{
    proxyServer.stop();
    commandServer.stop();
    testHelper.teardown();
  }
  
  @Test
  public void testCommandServletListVMs() throws InterruptedException, UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
           .field("command", "listvms")
           .asString();
    
    assertEquals(expectedListVMResponse, resp.getBody());
  }
  
  
  @Test
  public void testCommandServletBadCommand() throws InterruptedException, UnirestException{
    String badCommand = "xxyyzz";
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
           .field("command", badCommand)
           .asString();
    
    assertEquals(CommandServlet.badCommandMessage+badCommand, resp.getBody());
  }
  
  @Test
  public void testCommandServletNoCommand() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
        .asString();
    
    assertEquals(CommandServlet.noCommandMessage, resp.getBody());
  }
  
}
