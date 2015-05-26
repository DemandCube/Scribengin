package com.neverwinterdp.swing.scribengin;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.tool.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class ScribenginEmbeddedCluster extends ScribnginCluster {
  private ScribenginClusterBuilder clusterBuilder;
  private ScribenginEmbeddedClusterConfig    config;
  private ScribenginShell          shell;

  public ScribenginEmbeddedCluster(ScribenginEmbeddedClusterConfig config) {
    this.config = config ;
  }
  
  public ScribenginEmbeddedClusterConfig getClusterConfig() { return this.config ; }
  
  public ScribenginClusterBuilder getClusterBuilder() { return clusterBuilder; }

  public ScribenginEmbeddedClusterConfig getConfig() { return config; }

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