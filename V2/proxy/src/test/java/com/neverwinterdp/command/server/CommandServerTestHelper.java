package com.neverwinterdp.command.server;

import static org.junit.Assert.assertTrue;

import java.io.File;
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

public class CommandServerTestHelper {
  protected ZookeeperServerLauncher zkServerLauncher ;
  public String commandServerFolder = "./src/test/resources/commandServer";
  public String proxyServerFolder = "./src/test/resources/commandServer";
  public String commandServerXml = commandServerFolder+"/override-web.xml";
  public String proxyServerXml = proxyServerFolder+"/override-web.xml";
  
  public String getCommandServerFolder() {
    return commandServerFolder;
  }

  public String getProxyServerFolder() {
    return proxyServerFolder;
  }

  public String getCommandServerXml() {
    return commandServerXml;
  }

  public String getProxyServerXml() {
    return proxyServerXml;
  }

  
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  public void teardown() throws Exception {
    zkServerLauncher.shutdown();
  }

  protected <T> Injector newAppContainer() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    
    props.put("implementation:" + Registry.class.getName(), RegistryImpl.class.getName()) ;
    AppModule module = new AppModule(props) ;
    return Guice.createInjector(module);
  }
  
  protected Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault());
  }
  
  protected Shell newShell() throws RegistryException {
    return new Shell(new RegistryImpl(RegistryConfig.getDefault()).connect()) ;
  }
  
  public void assertWebXmlFilesExist(){
    //Check that web.xml exists
    File f = new File(commandServerXml);
    assertTrue(f.exists());
    f = new File(proxyServerXml);
    assertTrue(f.exists());
  }
}
