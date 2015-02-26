package com.neverwinterdp.jvmagent.registry;

import java.io.FileInputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.server.zookeeper.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;

public class RegistryAgentUnitTest {
private ZookeeperServerLauncher zkServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    zkServerLauncher.shutdown();
  }
  
  @Test
  public void testRegistryAgent() throws Exception {
    Properties props    = new Properties() ;
    props.load(new FileInputStream("src/main/plugin/agent.properties"));
    RegistryAgent agent = new RegistryAgent() ;
    agent.premain(props, null);
    Thread.sleep(3000);
    RegistryConfig config = RegistryConfig.getDefault();
    Registry registry = new RegistryImpl(config).connect();
    registry.get("/").dump(System.out);
  }
}
