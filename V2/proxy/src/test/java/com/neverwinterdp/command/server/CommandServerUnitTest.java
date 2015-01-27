package com.neverwinterdp.command.server;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class CommandServerUnitTest extends CommandServletUnitTest{
  static CommandServer cs;
  
  @BeforeClass
  public static void setup() throws Exception{
    //Bring up ZK and all that jazz
    testHelper = new CommandServerTestHelper();
    testHelper.assertWebXmlFilesExist();
    testHelper.setup();

    Registry registry = testHelper.getNewRegistry();
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    
    cs = new CommandServer(port);
    //Point our context to our web.xml we want to use for testing
    WebAppContext webapp = new WebAppContext();
    webapp.setResourceBase(testHelper.getCommandServerFolder());
    webapp.setDescriptor(testHelper.getCommandServerXml());
    cs.setHandler(webapp);
    cs.startServer();
  }
  
  @AfterClass
  public static void teardown() throws Exception{
    cs.stop();
    testHelper.teardown();
  }
}
