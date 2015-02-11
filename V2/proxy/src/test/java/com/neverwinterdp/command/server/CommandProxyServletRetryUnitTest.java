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
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;

public class CommandProxyServletRetryUnitTest {
  protected static JettyServer proxyServer;
  protected int commandPort = 8181;
  protected int commandPort2 = 8182;
  protected int proxyPort = 8383;
  Registry registry;
  String registryPath = "/vm/commandServer";
  
  Shell shell;
  VMClient vmClient;
  
  @Before
  public void setup() throws Exception{
    //Bring up ZK and all that jazz
    CommandServerTestBase.setup();

    registry = CommandServerTestBase.getNewRegistry();
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    registry.create(registryPath, ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    //Start proxy
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase(CommandServerTestBase.getProxyServerFolder());
    proxyApp.setDescriptor(  CommandServerTestBase.getProxyServerXml());
    
    proxyServer = new JettyServer(proxyPort, CommandProxyServlet.class);
    proxyServer.setHandler(proxyApp);
    proxyServer.start();
  }

  
  @After
  public void teardown() throws Exception{
    System.out.println("Stopping servers");
    proxyServer.stop();
    CommandServerTestBase.teardown();
  }
  
  @Test
  public void testProxyServletRetry() throws Exception{
    //Point our context to our web.xml we want to use for testing
    WebAppContext commandApp = new WebAppContext();
    commandApp.setResourceBase(CommandServerTestBase.getCommandServerFolder());
    commandApp.setDescriptor(CommandServerTestBase.getCommandServerXml());
    
    //Bring up commandServer using that context
    JettyServer commandServer = new JettyServer(commandPort, CommandServlet.class);
    commandServer.setHandler(commandApp);
    commandServer.start();
    
    //Test the proxy is working to begin with
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(proxyPort))
           .field("command", "vm list")
           .asString();
    
    assertEquals(CommandServerTestBase.expectedListVMResponse, resp.getBody());
    
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
    assertEquals(CommandServerTestBase.expectedListVMResponse, resp2.getBody());
    commandServer2.stop();
  }
  
}
