package com.neverwinterdp.scribengin;


import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.builder.HelloKafkaDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginAssertEventListener;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

abstract public class VMScribenginUnitTest {
  protected ScribenginClusterBuilder clusterBuilder ;
  protected long vmLaunchTime = 100;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    Thread.sleep(vmLaunchTime);
    clusterBuilder.startScribenginMasters();
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void testMaster() throws Exception {
    ScribenginShell shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    FileSystem fs = getFileSystem();
    try {
      HelloHDFSDataflowBuilder dataflowBuilder = new HelloHDFSDataflowBuilder(clusterBuilder, fs, getDataDir());
      dataflowBuilder.createSource(15, 3, 5);
      //HelloKafkaDataflowBuilder dataflowBuilder = new HelloKafkaDataflowBuilder(clusterBuilder);
      //dataflowBuilder.createSource(5, 10);

      ScribenginAssertEventListener sribenginAssert = dataflowBuilder.submit();
      sribenginAssert.waitForEvents(60000);
    } finally {
      Thread.sleep(3000);
      shell.execute("vm list");
      shell.execute("registry dump --path /");
      //HDFSUtil.dump(fs, getDataDir() + "/sink");
      //HDFSUtil.dump(fs, getDataDir() + "/invalid-sink");
    }
  }

  abstract protected String getDataDir() ;
  
  abstract protected FileSystem getFileSystem() throws Exception ;
  
  abstract protected VMClusterBuilder getVMClusterBuilder() throws Exception ;
}