package com.neverwinterdp.scribengin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;

import com.neverwinterdp.hadoop.MiniClusterUtil;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.environment.yarn.AppClient;
import com.neverwinterdp.vm.environment.yarn.YarnVMServicePlugin;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class VMScribenginYarnIntegrationTest extends VMScribenginUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  MiniYARNCluster miniYarnCluster ;
  MiniDFSCluster miniDFSCluster;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/hdfs", false);
    miniDFSCluster = MiniClusterUtil.createMiniDFSCluster("build/hdfs", 2) ;

    YarnConfiguration yarnConf = new YarnConfiguration() ;
    yarnConf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");
    miniYarnCluster = MiniClusterUtil.createMiniYARNCluster(yarnConf, 1);
    Configuration conf = miniYarnCluster.getConfig() ;
    super.setup();
    vmLaunchTime = 3000;
  }

  @After
  public void teardown() throws Exception {
    miniYarnCluster.stop();
    miniYarnCluster.close();
    miniDFSCluster.shutdown();
    super.teardown();
  }
  
  protected void createVMMaster(String name) throws Exception {
    System.out.println("fs.defaultFS = " + miniDFSCluster.getURI());
    String[] args = {
      "--environment", "YARN_MINICLUSTER",
      "--name", name,
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMServiceApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + YarnVMServicePlugin.class.getName(),
      "--yarn:yarn.resourcemanager.scheduler.address=localhost:8030",
      "--yarn:fs.defaultFS=" + miniDFSCluster.getURI()
    };
    AppClient appClient = new AppClient() ;
    appClient.run(args, new YarnConfiguration(miniYarnCluster.getConfig()));
  }
  
  protected void configureEnvironment(VMConfig vmConfig) {
    vmConfig.setEnvironment(VMConfig.Environment.YARN_MINICLUSTER);
    vmConfig.addYarnProperty("yarn.resourcemanager.scheduler.address", "localhost:8030");
    vmConfig.addYarnProperty("fs.defaultFS", miniDFSCluster.getURI().toString());
  }
  
  @Override
  protected FileSystem getFileSystem() throws Exception { 
    return miniDFSCluster.getFileSystem();
  }

  @Override
  protected String getDataDir() { return "/data"; }
}