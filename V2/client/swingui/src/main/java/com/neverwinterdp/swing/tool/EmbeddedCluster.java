package com.neverwinterdp.swing.tool;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class EmbeddedCluster extends Cluster {
  private ScribenginClusterBuilder clusterBuilder;
  private EmbeddedClusterConfig    config;
  private ScribenginShell          shell;

  public EmbeddedCluster(EmbeddedClusterConfig config) {
    this.config = config ;
  }
  
  public EmbeddedClusterConfig getClusterConfig() { return this.config ; }
  
  public ScribenginClusterBuilder getClusterBuilder() { return clusterBuilder; }

  public EmbeddedClusterConfig getConfig() { return config; }

  @Override
  public VMClient getVMClient() {
    if(clusterBuilder != null) {
      return clusterBuilder.getVMClusterBuilder().getVMClient();
    }
    return null ;
  }
  
  @Override
  public Registry getRegistry() {
    if(clusterBuilder != null) {
      return clusterBuilder.getVMClusterBuilder().getVMClient().getRegistry();
    }
    return null ;
  }
  
  @Override
  public ScribenginShell getScribenginShell() { return shell;}
  
  @Override
  public void startVMMaster() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(new EmbededVMClusterBuilder());
    clusterBuilder.clean();
    clusterBuilder.startVMMasters();
  }

  @Override
  public void shutdownVMMaster() throws Exception {
    clusterBuilder.shutdown();
    clusterBuilder.getVMClusterBuilder().shutdown();
    clusterBuilder = null ;
    shell = null ;
  }

  @Override
  public void startScribenginMaster() throws Exception {
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    clusterBuilder.startScribenginMasters();
  }

  @Override
  public void shutdownScribenginMaster() throws Exception {
    shell.getScribenginClient().shutdown();
  }
}