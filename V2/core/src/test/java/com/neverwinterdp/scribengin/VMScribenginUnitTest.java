package com.neverwinterdp.scribengin;


import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.junit.ScribenginAssert;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

abstract public class VMScribenginUnitTest {
  protected ScribenginClusterBuilder clusterBuilder ;
  protected ScribenginShell shell;
  protected HelloHDFSDataflowBuilder dataflowBuilder;
 
  private FileSystem fs;
  protected long vmLaunchTime = 100;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    Thread.sleep(vmLaunchTime);
    clusterBuilder.startScribenginMasters();
    fs = getFileSystem();
    dataflowBuilder = new HelloHDFSDataflowBuilder(clusterBuilder, fs, getDataDir());
    dataflowBuilder.createSource(15, 3, 5);
    HDFSUtil.dump(fs, getDataDir() + "/source");
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void testMaster() throws Exception {
    ScribenginAssert sribenginAssert = dataflowBuilder.submit();
    sribenginAssert.waitForEvents(60000);
    
    Thread.sleep(3000);
    shell.execute("vm list");
    shell.execute("registry dump --path /");
    HDFSUtil.dump(fs, getDataDir() + "/sink");
    HDFSUtil.dump(fs, getDataDir() + "/invalid-sink");
  }

  abstract protected String getDataDir() ;
  
  abstract protected FileSystem getFileSystem() throws Exception ;
  
  abstract protected VMClusterBuilder getVMClusterBuilder() throws Exception ;
}