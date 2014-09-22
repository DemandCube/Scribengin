package com.neverwinterdp.scribengin.kafka;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.scribengin.kafka.MiniDfsClusterBuilder;

/**
 * Brings up kafka, zookeeper, hadoop, and scribengin
 * @author Richard Duarte
 *
 */
public class ScribenginClusterBuilder {
  static {
    System.setProperty("app.dir", "build/cluster") ;
    System.setProperty("app.config.dir", "src/app/config") ;
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  public static String TOPICS = "cluster.test0,cluster.test1,cluster.test2";
  Server  scribenginServer ;
  Shell   shell ;

  public ScribenginClusterBuilder() throws Exception {
    scribenginServer = Server.create("-Pserver.name=scribengin", "-Pserver.roles=scribengin");
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
        " -Pscribengin:topics=" +TOPICS +
        " -Pscribengin:kafkaHost=127.0.0.1" +
        " -Pscribengin:kafkaPort=9092" +
        " -Pscribengin:workerCheckTimerInterval=5000" +
        " --member-role scribengin --autostart --module Scribengin \n";
      shell.executeScript(installScript);
      Thread.sleep(1000);
  }
  
  public void uninstall() {
    String uninstallScript = 
        "module uninstall --member-role scribengin --timeout 20000 --module Scribengin";
    shell.executeScript(uninstallScript);
  }
}