package com.neverwinterdp.swing.es;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.tool.server.EmbededElasticSearchServerSet;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.util.FileUtil;

public class ESCluster {
  final static ESCluster singleton = new ESCluster() ;
  
  private EmbededElasticSearchServerSet serverSet ;
  
  private  ESClient esclient ;

  public ESClient getESClient() { return this.esclient ; }
  
  public void connect(String ... connect) throws Exception {
    esclient = new ESClient(connect);
  }
  
  public void disconnect() throws Exception {
  }
  
  public void startEmbeddedCluster(final ESClusterConfiguration conf) throws Exception {
    FileUtil.removeIfExist(conf.getBaseDir(), false);
    Thread thread = new Thread() {
      public void run() {
        Map<String, String> props = new HashMap<>();
        serverSet = new EmbededElasticSearchServerSet(conf.getBaseDir(), conf.getBasePort(), conf.getNumOfInstances(), props) ;
        try {
          serverSet.start();
          Thread.sleep(15000);
          String[] connect = serverSet.getConnectString().split(",") ;
          connect(connect);
        } catch (Exception e) {
          MessageUtil.handleError(e);
        }
      }
    };
    thread.start() ;
  }
  
  public void shutdownEmbeddedCluster() throws Exception {
    if(serverSet != null) {
      serverSet.shutdown();
      serverSet = null ;
    }
  }
  
  static public ESCluster getInstance() { return singleton ; }
}
