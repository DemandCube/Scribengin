package com.neverwinterdp.swing.tool;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.client.VMClient;

public class RemoteCluster extends Cluster {
  static public RemoteCluster INSTANCE = new RemoteCluster() ;

  private ScribenginShell shell;
  
  public void connect(RegistryConfig config) throws Exception {
    Registry registry = new RegistryImpl(config);
    registry.connect(1 * 30 * 1000);
    VMClient vmClient = new VMClient(registry);
    vmClient.setWaitForResultTimeout(60000);
    shell = new ScribenginShell(vmClient) ;
  }

  public void disconnect() throws Exception {
    if(shell == null) return ;
    shell.getScribenginClient().getRegistry().disconnect();
    shell = null ;
  }

  
  @Override
  public void startVMMaster() throws Exception {
  }

  @Override
  public void shutdownVMMaster() throws Exception {
  }

  @Override
  public void startScribenginMaster() throws Exception {
  }

  @Override
  public void shutdownScribenginMaster() throws Exception {
  }

  @Override
  public ScribenginShell getScribenginShell() { return shell; }

  @Override
  public VMClient getVMClient() {
    if(shell == null) return null ;
    return shell.getVMClient();
  }

  @Override
  public Registry getRegistry() {
    if(shell == null) return null ;
    return shell.getVMClient().getRegistry();
  }
}