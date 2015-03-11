package com.neverwinterdp.scribengin;


import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.test.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.test.HelloKafkaDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSSourceGenerator;
import com.neverwinterdp.scribengin.kafka.KafkaSourceGenerator;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

abstract public class ScribenginUnitTest {
  protected ScribenginClusterBuilder clusterBuilder ;
  protected long vmLaunchTime = 100;
  protected ScribenginShell shell;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
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
    
    clusterBuilder.getScribenginClient().shutdown();
    shell.execute("vm info");
    shell.execute("registry dump --path /");
  }
  
  void testHdfsDataflow() throws Exception {
    FileSystem fs = getFileSystem();
    try {
      HelloHDFSDataflowBuilder hdfDataflowBuilder = 
          new HelloHDFSDataflowBuilder(clusterBuilder.getScribenginClient(), getDataDir());
      new HDFSSourceGenerator().generateSource(fs, getDataDir() + "/source");
      ScribenginWaitingEventListener eventListener = hdfDataflowBuilder.submit();
      eventListener.waitForEvents(60000);
    } finally {
      Thread.sleep(3000);
      shell.execute("vm info");
      shell.execute("registry dump --path /");
      HDFSUtil.dump(fs, getDataDir() + "/sink");
      HDFSUtil.dump(fs, getDataDir() + "/invalid-sink");
    }
  }

  void testKafkaDataflow() throws Exception {
    try {
      KafkaSourceGenerator generator = new KafkaSourceGenerator("hello", "127.0.0.1:2181");
      generator.generateAndWait("hello.source");
      HelloKafkaDataflowBuilder kafkaDataflowBuilder = new HelloKafkaDataflowBuilder(clusterBuilder.getScribenginClient());
      ScribenginWaitingEventListener sribenginAssert = kafkaDataflowBuilder.submit();
      sribenginAssert.waitForEvents(60000);
    } finally {
      Thread.sleep(3000);
      shell.execute("vm info");
      shell.execute("registry dump --path /");
    }
  }
  
  abstract protected String getDataDir() ;
  
  abstract protected FileSystem getFileSystem() throws Exception ;
  
  abstract protected VMClusterBuilder getVMClusterBuilder() throws Exception ;
}