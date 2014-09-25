package com.neverwinterdp.scribengin.clusterBuilder;

import java.io.IOException;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;
import com.neverwinterdp.util.FileUtil;

/**
 * Brings up kafka, zookeeper, hadoop
 * @author Richard Duarte
 *
 */
public class SupportClusterBuilder {
  static {
    System.setProperty("app.dir", "build/cluster") ;
    System.setProperty("app.config.dir", "src/app/config") ;
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  private static String MINI_CLUSTER_PATH = "/tmp/miniCluster";
  UnitTestCluster hadoopServer;
  Server  zkServer, kafkaServer ;
  Shell   shell ;
  String hadoopConnection="";

  public SupportClusterBuilder() throws Exception {
    FileUtil.removeIfExist("build/cluster", false);
    zkServer = Server.create("-Pserver.name=zookeeper", "-Pserver.roles=zookeeper") ;
    kafkaServer = Server.create("-Pserver.name=kafka", "-Pserver.roles=kafka") ;
    shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    Thread.sleep(1000);
    hadoopServer = UnitTestCluster.instance(MINI_CLUSTER_PATH);
  }

  public Shell getShell() { return this.shell ; }
  
  public String getHadoopConnection() { return this.hadoopConnection; }
  
  public void destroy() throws Exception {
    shell.close() ; 
    kafkaServer.destroy();
    zkServer.destroy();
    hadoopServer.destroy();
  }
  
  public void install() throws InterruptedException, IOException {
    hadoopServer.build(3);
    hadoopConnection = hadoopServer.getUrl();
    
    String installScript =
        "module install " + 
        " -Pmodule.data.drop=true" +
        " -Pzk:clientPort=2181 " +
        " --member-role zookeeper --autostart --module Zookeeper \n" +
        
        "module install " +
        " -Pmodule.data.drop=true" +
        " -Pkafka:port=9092 -Pkafka:zookeeper.connect=127.0.0.1:2181 " +
        " --member-role kafka --autostart --module Kafka \n" +
        
        "module install " +
        " -Pmodule.data.drop=true -Pkafka:zookeeper.connect=127.0.0.1:2181 " +
        " --member-role kafka --autostart --module KafkaConsumer\n";
      shell.executeScript(installScript);
      Thread.sleep(1000);
  }
  
  public void uninstall() {
    
    String uninstallScript = 
        "module uninstall --member-role kafka --timeout 40000 --module KafkaConsumer \n" +
        "module uninstall --member-role kafka --timeout 40000 --module Kafka \n" +
        "module uninstall --member-role zookeeper --timeout 20000 --module Zookeeper \n";
    shell.executeScript(uninstallScript);
  }
}