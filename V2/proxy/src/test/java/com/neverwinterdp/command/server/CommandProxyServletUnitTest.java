package com.neverwinterdp.command.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

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
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;

public class CommandProxyServletUnitTest {
  protected static JettyServer commandServer;
  protected static JettyServer proxyServer;
  protected static int commandPort = 8181;
  protected static int proxyPort = 8383;
  
  static Shell shell;
  static VMClient vmClient;
  
  @BeforeClass
  public static void setup() throws Exception{
    //Bring up ZK and all that jazz
    CommandServerTestBase.setup();

    Registry registry = CommandServerTestBase.getNewRegistry();
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    registry.create("/vm/commandServer", ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext commandApp = new WebAppContext();
    commandApp.setResourceBase(CommandServerTestBase.getCommandServerFolder());
    commandApp.setDescriptor(CommandServerTestBase.getCommandServerXml());
    
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase(CommandServerTestBase.getProxyServerFolder());
    proxyApp.setDescriptor(CommandServerTestBase.getProxyServerXml());
    
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
    CommandServerTestBase.teardown();
  }
  
  @Test
  public void testCommandServletListVMs() throws InterruptedException, UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
           .field("command", "vm list")
           .asString();
    
    assertEquals(CommandServerTestBase.expectedListVMResponse, resp.getBody());
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
    assertNotNull(resp.getBody());
    assertFalse(resp.getBody().equals(""));
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
    
    assertEquals("", resp.getBody());
  }
  
  @Test
  public void testCommandServletNoCommand() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
        .asString();
    
    assertEquals(CommandServlet.noCommandMessage, resp.getBody());
  }
}
