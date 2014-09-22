package com.neverwinterdp.scribengin.kafka;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;

/**
 * Brings up scribengin
 * @author Richard Duarte
 *
 */
public class ScribenginWorkerClusterBuilder {
  static {
    System.setProperty("app.dir", "build/cluster") ;
    System.setProperty("app.config.dir", "src/app/config") ;
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  public static String TOPIC = "cluster.test" ;
  Server  scribenginServer ;
  Shell   shell ;
  String hadoopConnection="";

  public ScribenginWorkerClusterBuilder(String hadoopConnect) throws Exception {
    this.hadoopConnection = hadoopConnect;
    scribenginServer = Server.create("-Pserver.name=scribenginworker", "-Pserver.roles=scribenginworker");
    shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    Thread.sleep(1000);
  }

  public Shell getShell() { return this.shell ; }
  
  
  
  public void destroy() throws Exception {
    shell.close() ; 
    scribenginServer.destroy();
  }
  
  public void install() throws InterruptedException {
    String installScript =
        "module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribenginworker:checkpointinterval=200" +
        " -Pscribenginworker:leaderHost=127.0.0.1" +
        " -Pscribenginworker:leaderPort=9092" +
        " -Pscribenginworker:partition=0" +
        " -Pscribenginworker:topic="+TOPIC +
        " -Pscribenginworker:hdfsPath="+hadoopConnection+
        " -Pscribenginworker:preCommitPathPrefix=/tmp"+
        " -Pscribenginworker:commitPathPrefix=/committed"+
        " --member-role scribenginworker --autostart --module ScribenginWorker \n";
      shell.executeScript(installScript);
      Thread.sleep(1000);
  }
  
  public void uninstall() {
    
    String uninstallScript = 
        "module uninstall --member-role scribengin --timeout 20000 --module Scribengin";
    shell.executeScript(uninstallScript);
  }
}