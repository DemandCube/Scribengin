package com.neverwinterdp.es.tool.server;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.es.Configuration;
import com.neverwinterdp.es.ElasticSearchService;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.module.MycilaJmxModuleExt;
import com.neverwinterdp.tool.server.Server;
import com.neverwinterdp.util.LoggerFactory;

public class ElasticSearchServer implements Server {
  private String nodeName ;
  private String hostname = "localhost";
  private int    port     = 9300 ;
  private String dataDir  = null ;
  private ElasticSearchServerThread elasticServiceThread;
  
  public ElasticSearchServer(String nodeName, String hostname, int port, String dataDir) {
    this.nodeName = nodeName; 
    this.hostname = hostname;
    this.port = port;
    this.dataDir = dataDir ;
  }
  
  @Override
  public String getHost() { return hostname; }

  @Override
  public int getPort() { return port; }

  @Override
  public String getConnectString() { return hostname + ":" + port; }

  public Logger getLogger() {
    if(elasticServiceThread == null) return null ;
    return elasticServiceThread.elasticService.getLogger();
  }
  
  @Override
  public void start() throws Exception {
    elasticServiceThread = new ElasticSearchServerThread() ;
    elasticServiceThread.start();
  }

  @Override
  public void shutdown() {
    if(elasticServiceThread != null) {
      elasticServiceThread.elasticService.stop();
    }
  }
  
  public class ElasticSearchServerThread extends Thread {
    private ElasticSearchService elasticService ;
    
    public void run() {
      try {
        String[] args = {
            "--es:server.name=" + hostname,
            "--es:path.data=" + dataDir,
            "--es:transport.tcp.port=" + port
        } ;
        final Configuration configuration = new Configuration() ;
        new JCommander(configuration, args);
        AppModule module = new AppModule(new HashMap<String, String>()) {
          @Override
          protected void configure(Map<String, String> properties) {
            bindMapProperties("esProperties", configuration.getESProperties()) ;
            bindInstance(LoggerFactory.class, new LoggerFactory("elasticsearch")) ;
          };
        };
        Module[] modules = {
            new CloseableModule(),new Jsr250Module(), new MycilaJmxModuleExt(nodeName),  module
        };
        Injector container = Guice.createInjector(Stage.PRODUCTION, modules);
        elasticService = container.getInstance(ElasticSearchService.class);
        elasticService.start();
        Thread.currentThread().join();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
