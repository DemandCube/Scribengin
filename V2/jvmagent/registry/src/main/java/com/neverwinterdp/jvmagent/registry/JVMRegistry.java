package com.neverwinterdp.jvmagent.registry;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;

public class JVMRegistry {
  final static public String JVM_REGISTRY_PATH = "/cluster/jvm-registry";
  
  private Registry registry ;
  
  public JVMRegistry(RegistryAgentConfig config) throws RegistryException {
    RegistryConfig registryConfig = RegistryConfig.getDefault();
    registryConfig.setConnect(config.getZookeeperConnect());
    registry = new RegistryImpl(registryConfig);
    registry.connect();
    registry.createIfNotExist(JVM_REGISTRY_PATH) ;
  }
  
  public Registry getRegistry() { return this.registry ; }
  
  public void create(JVMInfo info) throws RegistryException {
    Node jvmRegistryNode = registry.get(JVM_REGISTRY_PATH);
    Node jvmNode = jvmRegistryNode.createChild(info.getHostname(), NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    jvmNode.setData(info);
  }
}
