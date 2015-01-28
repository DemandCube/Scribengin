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
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;

public class CommandServletUnitTest {
  protected static JettyServer commandServer;
  protected static int port = 8181;

  
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

    Registry registry = CommandServerTestHelper.getNewRegistry();
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext webapp = new WebAppContext();
    webapp.setResourceBase(testHelper.getCommandServerFolder());
    webapp.setDescriptor(testHelper.getCommandServerXml());
    
    //Bring up commandServer using that context
    commandServer = new JettyServer(port, CommandServlet.class);
    commandServer.setHandler(webapp);
    commandServer.start();
  }
  
  
  @AfterClass
  public static void teardown() throws Exception{
    //Uncomment this line if you want the server to not exit
    //commandServer.join();

    commandServer.stop();
    testHelper.teardown();
  }
  
  @Test
  public void testCommandServletListVMs() throws InterruptedException, UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(port))
           .field("command", "vm list")
           .asString();
    
    //assertEquals("command run: "+"listvms", resp.getBody());
    assertEquals(CommandServerTestHelper.expectedListVMResponse, resp.getBody());
  }
  
  @Test
  public void testCommandServletBadCommand() throws InterruptedException, UnirestException{
    String badCommand = "xxyyzz";
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(port))
           .field("command", badCommand)
           .asString();
    
    assertEquals(CommandServlet.badCommandMessage+badCommand, resp.getBody());
  }
  
  @Test
  public void testCommandServletNoCommand() throws UnirestException{
    HttpResponse<String> resp = Unirest.post("http://localhost:"+Integer.toString(port))
        .asString();
    
    assertEquals(CommandServlet.noCommandMessage, resp.getBody());
  }
}
