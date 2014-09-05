package com.neverwinterdp.scribengin;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;

public class ScribenginClusterServiceUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/main/resources/log4j.properties") ;
  }
  
  private static Server ScribeServer ;
  private static Shell shell;
  
  @BeforeClass
  public static void setup() throws Exception {
    ScribeServer = Server.create("-Pserver.name=scribengin", "-Pserver.roles=scribengin");
    shell = new Shell();
    shell.getShellContext().connect();
    String installScript =
        "module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribengin:example=\"overridingdefaultvalue!\" " +
        " --member-role scribengin --autostart --module Scribengin \n";
    shell.executeScript(installScript);
  }
  
  @AfterClass
  public static void teardown() {
    String uninstallScript = 
        "module uninstall --member-role scribengin --timeout 40000 --module scribengin \n";
    shell.executeScript(uninstallScript);
    shell.close();
    ScribeServer.destroy() ;
  }
  
  @Test
  public void test(){
    //Do some stuff to test the server here...
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }}
}
