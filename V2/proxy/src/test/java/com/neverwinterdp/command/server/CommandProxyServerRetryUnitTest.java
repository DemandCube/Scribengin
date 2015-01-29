package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;

import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RegistryException;

public class CommandProxyServerRetryUnitTest extends CommandProxyServletRetryUnitTest{
  CommandProxyServer cps;
  
  @Before
  public void setup() throws Exception{
    CommandServerTestBase.setup();
    
    registry = CommandServerTestBase.getNewRegistry();
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    //Add the entry to tell the proxy server where to go to find the commandServer
    registry.create(registryPath, ("http://localhost:"+Integer.toString(commandPort)).getBytes(), NodeCreateMode.PERSISTENT);
    
    
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext proxyApp = new WebAppContext();
    proxyApp.setResourceBase(CommandServerTestBase.getProxyServerFolder());
    proxyApp.setDescriptor(CommandServerTestBase.getProxyServerXml());
    
    //Bring up proxy
    cps = new CommandProxyServer(proxyPort);
    cps.setHandler(proxyApp);
    cps.startServer();
  }
  
  

  @After
  public void teardown() throws Exception{
    System.out.println("Stopping servers");
    cps.stop();
    CommandServerTestBase.teardown();
  }
}
