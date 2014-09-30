package com.neverwinterdp.scribengin.cluster;

import org.junit.Test;

import com.neverwinterdp.scribengin.ScribeRemoteCommand;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.cluster.ClusterClient;
import com.neverwinterdp.server.cluster.ClusterMember;
import com.neverwinterdp.server.cluster.hazelcast.HazelcastClusterClient;
import com.neverwinterdp.server.command.ServerCommandResult;
import com.neverwinterdp.server.shell.Shell;
import com.neverwinterdp.util.text.TabularFormater;

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
    
    String installScript ="module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribemaster:topics=topictopictopic" +
        " --member-role scribemaster --autostart --module ScribeMaster \n";
    shell.executeScript(installScript);
    Thread.sleep(2000);
    
    
    ScribeRemoteCommand scr = new ScribeRemoteCommand("HELLO LONDON");
    scr.setTimeout(5000);
    ClusterClient clusterClient = new HazelcastClusterClient() ;
    ServerCommandResult<String>[] results = null ;
    ClusterMember[] members = clusterClient.findClusterMemberByRole("scribemaster") ;
    results = clusterClient.execute(scr, members) ;
    
    String[] header = { "Server", "Listen IP:PORT", "Return Message" };
    TabularFormater formater = new TabularFormater(header);
    for (ServerCommandResult<String> result : results) {
      formater.addRow(result.getFromMember().getMemberName(), result.getFromMember(), result.getResult());
    }
    System.err.println(formater.getFormatText());
    
    
    scribeMaster.destroy();
  }
}
