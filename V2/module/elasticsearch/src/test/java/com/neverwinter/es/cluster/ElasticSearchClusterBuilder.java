package com.neverwinter.es.cluster;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;
import com.neverwinterdp.util.FileUtil;

public class ElasticSearchClusterBuilder {
  static {
    System.setProperty("app.dir", "build/cluster") ;
    System.setProperty("app.config.dir", "src/app/config") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  static String TOPIC = "metrics.consumer" ;
  
  Server[]  esServer ;
  Shell   shell ;

  public ElasticSearchClusterBuilder() throws Exception {
    FileUtil.removeIfExist("build/cluster", false);
    esServer = new Server[2] ;
    for(int i = 0; i < esServer.length; i++) {
      esServer[i] = Server.create("-Pserver.name=elasticsearch" + (i + 1), "-Pserver.roles=elasticsearch") ;
    }
    
    shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    Thread.sleep(1000);
  }

  public Shell getShell() { return this.shell ; }
  
  public void destroy() throws Exception {
    shell.close() ; 
    for(int i = 0; i < esServer.length; i++) {
      esServer[i].destroy();
    }
  }
  
  public void install() throws InterruptedException {
    String installScript =
        "module install " + 
        " -Pmodule.data.drop=true" +
        " -Pes:cluster.name=neverwinterdp " +
        " --member-role elasticsearch --timeout 30000 --autostart --module ElasticSearch" ;
      shell.executeScript(installScript);
      Thread.sleep(1000);
  }
  
  public void uninstall() {
    String uninstallScript = 
        "module uninstall --member-role elasticsearch --timeout 30000 --module ElasticSearch";
    shell.executeScript(uninstallScript);
  }
}