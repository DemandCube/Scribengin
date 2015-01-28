package com.neverwinterdp.scribengin;


import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.builder.HelloKafkaDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
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
  public void testDataflows() throws Exception {
    long start = System.currentTimeMillis() ;
    testHdfsDataflow();
    long hdfsDataflowExecTime = System.currentTimeMillis() - start ;

    start = System.currentTimeMillis() ;
    testKafkaDataflow();
    long kafkaDataflowExecTime = System.currentTimeMillis() - start ;
    System.out.println("Test hello hdfs dataflow in " + hdfsDataflowExecTime + "ms");
    System.out.println("Test hello kafka dataflow in " + kafkaDataflowExecTime + "ms");
  }
  
  void testHdfsDataflow() throws Exception {
    ScribenginShell shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    FileSystem fs = getFileSystem();
    try {
      HelloHDFSDataflowBuilder hdfDataflowBuilder = 
          new HelloHDFSDataflowBuilder(clusterBuilder.getScribenginClient(), fs, getDataDir());
      hdfDataflowBuilder.createSource(15, 3, 5);
      ScribenginWaitingEventListener eventListener = hdfDataflowBuilder.submit();
      eventListener.waitForEvents(60000);
    } finally {
      Thread.sleep(3000);
      shell.execute("vm list");
      shell.execute("registry dump --path /");
      HDFSUtil.dump(fs, getDataDir() + "/sink");
      HDFSUtil.dump(fs, getDataDir() + "/invalid-sink");
    }
  }

  void testKafkaDataflow() throws Exception {
    ScribenginShell shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
    try {
      HelloKafkaDataflowBuilder kafkaDataflowBuilder = 
          new HelloKafkaDataflowBuilder(clusterBuilder.getScribenginClient());
      kafkaDataflowBuilder.createSource(5, 10);
      ScribenginWaitingEventListener sribenginAssert = kafkaDataflowBuilder.submit();
      sribenginAssert.waitForEvents(60000);
    } finally {
      Thread.sleep(3000);
      shell.execute("vm list");
      shell.execute("registry dump --path /");
    }
  }
  
  abstract protected String getDataDir() ;
  
  abstract protected FileSystem getFileSystem() throws Exception ;
  
  abstract protected VMClusterBuilder getVMClusterBuilder() throws Exception ;
}