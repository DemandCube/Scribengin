package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.event.VMAssertEventListener;

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
  protected String expectedRegistryDumpResponse=
                    "/\n"+
                    "  vm\n"+
                    "    history\n"+
                    "    allocated\n"+
                    "      vm-master-1 - {\"storedPath\":\"/vm/allocated/vm-master-1\",\"memory\":128,\"cpuCores\":1,\"hostname\n"+
                    "        status - \"RUNNING\"\n"+
                    "          heartbeat\n"+
                    "        commands\n"+
                    "    commandServer - http://localhost:8181\n"+
                    "    master\n"+
                    "      leader - {\"path\":\"/vm/allocated/vm-master-1\"}\n"+
                    "        leader-0000000000\n";
  
  
  //Used to bring up VMs to test with
  static CommandServerTestHelper testHelper;
  static Shell shell;
  static VMClient vmClient;
  
  @BeforeClass
  public static void setup() throws Exception{
    //Bring up ZK and all that jazz
    testHelper = new CommandServerTestHelper();
    testHelper.assertWebXmlFilesExist();
    testHelper.setup();

    Registry registry = new RegistryImpl(RegistryConfig.getDefault());
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    //Launch a single VM
    shell = testHelper.newShell();
    VMAssertEventListener vmAssert = new VMAssertEventListener(registry);
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING);
    //VM vmMaster1 = createVMMaster("vm-master-1");
    CommandServletUnitTest.createVMMaster("vm-master-1");
    vmAssert.waitForEvents(5000);
    
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    registry.create("/vm/commandServer", ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext commandApp = new WebAppContext();
    commandApp.setResourceBase(testHelper.getCommandServerFolder());
    commandApp.setDescriptor(testHelper.getCommandServerXml());
    
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase(testHelper.getProxyServerFolder());
    proxyApp.setDescriptor(testHelper.getProxyServerXml());
    
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
           .field("command", "vm list")
           .asString();
    
    assertEquals(expectedListVMResponse, resp.getBody());
  }
  
  @Test
  public void testCommandScribenginMaster() throws InterruptedException, UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
           .field("command", "scribengin master")
           .asString();
    
    assertEquals("", resp.getBody());
  }
  
  @Test
  public void testCommandServletDumpRegistry() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
        .field("command", "registry dump")
        .asString();
 
    assertEquals(expectedRegistryDumpResponse, resp.getBody());
  }
  
  @Test
  @Ignore
  public void testCommandServletDumpRegistryWithPath() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
        .field("command", "registry dump")
        .field("path", "/vm/commandServer")
        .asString();
    System.err.println("RESP: "+resp.getBody());
    assertEquals("http://localhost:"+Integer.toString(commandPort), resp.getBody());
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
