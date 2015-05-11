package com.neverwinterdp.swing.tool;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.client.VMClient;

public class RemoteCluster {
  static public RemoteCluster INSTANCE = new RemoteCluster() ;

  public void connect(RegistryConfig config) {
    Registry registry = new RegistryImpl(config);
    VMClient vmClient = new VMClient(registry);
    ScribenginShell shell = new ScribenginShell(vmClient) ;
  }
}
