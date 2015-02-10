package com.neverwinterdp.scribengin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

public class ScribenginRandomFailureUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  protected ScribenginClusterBuilder clusterBuilder ;
  protected ScribenginShell shell;
  
  @Before
  public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder(getVMClusterBuilder()) ;
    clusterBuilder.clean(); 
    clusterBuilder.startVMMasters();
    Thread.sleep(3000);
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }
  
  @After
  public void teardown() throws Exception {
    clusterBuilder.shutdown();
  }
  
  @Test
  public void testDataflows() throws Exception {
    shell.execute(
     "dataflow-test kafka " + 
     "  --worker 1 --executor-per-worker 3 --duration 300000 " +
     "  --kafka-num-partition 5 --kafka-write-period 5 --kafka-max-message-per-partition 1000"
    );
    Thread.sleep(3000);
    shell.execute("vm info");
    shell.execute("scribengin info");
    shell.execute("dataflow info --history hello-kafka-dataflow-0");
  }
  
  protected FileSystem getFileSystem() throws Exception { return FileSystem.get(new Configuration()); }

  protected String getDataDir() { return "./build/hdfs"; }
  
  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }
}