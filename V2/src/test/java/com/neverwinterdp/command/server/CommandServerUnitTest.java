package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.junit.VMAssert;

public class CommandServerUnitTest extends CommandServletUnitTest{
  static CommandServer cs;
  
  @BeforeClass
  public static void setup() throws Exception{
    //Bring up ZK and all that jazz
    testHelper = new VMUnitTest();
    testHelper.setup();

    //Launch a single VM
    shell = testHelper.newShell();
    VMAssert vmAssert = new VMAssert(shell.getVMClient());
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING);
    //VM vmMaster1 = createVMMaster("vm-master-1");
    createVMMaster("vm-master-1");
    vmAssert.waitForEvents(5000);

    cs = new CommandServer(port);
    //Point our context to our web.xml we want to use for testing
    WebAppContext webapp = new WebAppContext();
    webapp.setResourceBase("./src/test/resources/commandServer");
    webapp.setDescriptor("./src/test/resources/commandServer/override-web.xml");
    cs.setHandler(webapp);
    cs.startServer();
  }
  
  @AfterClass
  public static void teardown() throws Exception{
    cs.stop();
    testHelper.teardown();
  }
}
