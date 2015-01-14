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
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.environment.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.junit.VMAssert;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class TestCommandServer {
  private static JettyServer commandServer;
  private static int port = 8181;
  private String expectedListVMResponse = 
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
    createVMMaster("vm-master-1");
    vmAssert.waitForEvents(5000);
    
    //Point our context to our web.xml we want to use for testing
    WebAppContext webapp = new WebAppContext();
    webapp.setResourceBase("./src/test/resources/");
    webapp.setDescriptor("./src/test/resources/override-web.xml");
    
    //Bring up commandServer using that context
    commandServer = new JettyServer(port, CommandServlet.class);
    commandServer.setHandler(webapp);
    commandServer.start();
  }
  
  private static VM createVMMaster(String name) throws Exception {
    String[] args = {
      "--name", name,
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMServiceApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + JVMVMServicePlugin.class.getName()
    };
    VM vm = VM.run(args);
    return vm;
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
           .field("command", "listvms")
           .asString();
    
    //assertEquals("command run: "+"listvms", resp.getBody());
    assertEquals(expectedListVMResponse, resp.getBody());
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
