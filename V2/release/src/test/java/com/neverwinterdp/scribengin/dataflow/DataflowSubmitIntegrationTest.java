package com.neverwinterdp.scribengin.dataflow;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.hadoop.MiniClusterUtil;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.YarnVMClient;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class DataflowSubmitIntegrationTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  protected ScribenginClusterBuilder clusterBuilder ;
  MiniYARNCluster miniYarnCluster ;
  MiniDFSCluster miniDFSCluster;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("build/hdfs", false);
    miniDFSCluster = MiniClusterUtil.createMiniDFSCluster("build/hdfs", 2) ;

    YarnConfiguration yarnConf = new YarnConfiguration() ;
    yarnConf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");
    miniYarnCluster = MiniClusterUtil.createMiniYARNCluster(yarnConf, 1);
    
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    Thread.sleep(5000);
    clusterBuilder.startScribenginMasters();
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
    
    miniYarnCluster.stop();
    miniYarnCluster.close();
    miniDFSCluster.shutdown();
  }
  
  @Test
  public void testHdfsDataflow() throws Exception {
    ScribenginShell shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    FileSystem fs = getFileSystem();
    try {
      HelloHDFSDataflowBuilder hdfDataflowBuilder = 
        new HelloHDFSDataflowBuilder(clusterBuilder.getScribenginClient(), fs, getDataDir());
      hdfDataflowBuilder.createSource(15, 3, 5);
      ScribenginClient scribenginClient = clusterBuilder.getScribenginClient();
      String dataflowJson = IOUtil.getFileContentAsString("src/dataflows/hdfs/dataflow.json") ;
      ScribenginWaitingEventListener eventListener =
        scribenginClient.submit("build/release/dataflows/hdfs", dataflowJson);
      eventListener.waitForEvents(60000); 
    } catch(Exception ex) {
      ex.printStackTrace();
    } finally {
      Thread.sleep(3000);
      shell.execute("vm info");
      shell.execute("registry dump --path /");
      HDFSUtil.dump(fs, getDataDir() + "/sink");
      HDFSUtil.dump(fs, getDataDir() + "/invalid-sink");
    }
  }
  
  protected FileSystem getFileSystem() throws Exception { 
    return miniDFSCluster.getFileSystem();
  }

  protected String getDataDir() { return "/data"; }
  
  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    HadoopProperties yarnProps = new HadoopProperties() ;
    yarnProps.put("yarn.resourcemanager.scheduler.address", "localhost:8030");
    yarnProps.put("fs.defaultFS", miniDFSCluster.getURI().toString());
    Registry registry = new RegistryImpl(RegistryConfig.getDefault());
    YarnVMClient vmClient = new YarnVMClient(registry, yarnProps,miniYarnCluster.getConfig());
    vmClient.setLocalAppHome("build/release/scribengin");
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder(vmClient) ;
    return builder;
  }
}
