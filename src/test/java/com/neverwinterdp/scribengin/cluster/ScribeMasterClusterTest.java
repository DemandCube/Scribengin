package com.neverwinterdp.scribengin.cluster;

import org.junit.Test;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;

public class ScribeMasterClusterTest {
  static Server scribeMaster;
  static Server scribeMaster2;
  
  @Test
  public void ScribeConsumerClusterTest() throws InterruptedException {
    //Bring up scribeMaster
    scribeMaster = Server.create("-Pserver.name=scribemaster", "-Pserver.roles=scribemaster");
    scribeMaster2 = Server.create("-Pserver.name=scribemaster2", "-Pserver.roles=scribemaster");
    Shell shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    
    //TODO: Finish this script
    String installScript ="module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribemaster:topics=topictopictopic" +
        " --member-role scribemaster --autostart --module ScribeMaster \n";
    shell.executeScript(installScript);
    Thread.sleep(2000);
    
    
    
    scribeMaster.destroy();
  }
}
