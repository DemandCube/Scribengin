package com.neverwinterdp.swing.tool;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class Cluster {
  static private Cluster INSTANCE = new Cluster() ;
  
  
  private ScribenginClusterBuilder clusterBuilder;
  private ClusterConfig            config = new ClusterConfig();
  private ScribenginShell          shell;

  public ClusterConfig getClusterConfig() { return this.config ; }
  
  public ScribenginClusterBuilder getClusterBuilder() {
    return clusterBuilder;
  }

  public ClusterConfig getConfig() {
    return config;
  }

  public VMClient getVMClient() {
    if(clusterBuilder != null) {
      return clusterBuilder.getVMClusterBuilder().getVMClient();
    }
    return null ;
  }
  
  public Registry getRegistry() {
    if(clusterBuilder != null) {
      return clusterBuilder.getVMClusterBuilder().getVMClient().getRegistry();
    }
    return null ;
  }
  
  public ScribenginShell getScribenginShell() { return shell;}

  public void launch() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();

    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }

  public void shutdown() throws Exception {
    clusterBuilder.shutdown();
    clusterBuilder = null ;
    shell = null ;
  }

  static public Cluster getInstance() { return INSTANCE; }
}
