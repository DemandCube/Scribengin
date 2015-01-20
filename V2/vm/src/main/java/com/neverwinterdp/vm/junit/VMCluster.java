package com.neverwinterdp.vm.junit;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.client.shell.Shell;

public class VMCluster {
  private String baseDir = "./build/data";
  protected ZookeeperServerLauncher zkServerLauncher ;
  
  public VMCluster() { }
  
  public VMCluster(String baseDir) { 
    this.baseDir = baseDir ;
  }
  
  public void clean() throws Exception {
    FileUtil.removeIfExist(baseDir, false);
  }
  
  public void start() throws Exception {
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  public void shutdown() throws Exception {
    zkServerLauncher.shutdown();
  }

  public <T> Injector newAppContainer() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    
    props.put("implementation:" + Registry.class.getName(), RegistryImpl.class.getName()) ;
    AppModule module = new AppModule(props) ;
    return Guice.createInjector(module);
  }
  
  public Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault());
  }
  
  public Shell newShell() throws RegistryException {
    return new Shell(new RegistryImpl(RegistryConfig.getDefault()).connect()) ;
  }
}
