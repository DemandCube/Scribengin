package com.neverwinterdp.scribengin;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;

import com.neverwinterdp.hadoop.MiniClusterUtil;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.YarnVMClient;

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
    vmLaunchTime = 3000;
    super.setup();
  }

  @After
  public void teardown() throws Exception {
    miniYarnCluster.stop();
    miniYarnCluster.close();
    miniDFSCluster.shutdown();
    super.teardown();
  }
  
  @Override
  protected FileSystem getFileSystem() throws Exception { 
    return miniDFSCluster.getFileSystem();
  }

  @Override
  protected String getDataDir() { return "/data"; }
  
  @Override
  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    Map<String, String> yarnProps = new HashMap<>() ;
    yarnProps.put("yarn.resourcemanager.scheduler.address", "localhost:8030");
    yarnProps.put("fs.defaultFS", miniDFSCluster.getURI().toString());
    Registry registry = new RegistryImpl(RegistryConfig.getDefault());
    YarnVMClient vmClient = new YarnVMClient(registry, yarnProps,miniYarnCluster.getConfig());
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder(vmClient) ;
    return builder;
  }
}