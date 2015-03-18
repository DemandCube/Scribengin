package com.neverwinterdp.scribengin;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

public class DataflowHdfsToHdfsUnitTest {
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
    clusterBuilder.startScribenginMasters();
    shell = new ScribenginShell(clusterBuilder.getVMClusterBuilder().getVMClient());
  }
  
  @After
  public void teardown() throws Exception {
    try{
    clusterBuilder.shutdown();
    }catch(Exception e){
  
    }
  }
  
  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }
  
  @Test
  public void testDataflows() throws Exception {
    String command = 
        "dataflow-test hdfs " + 
        "  --numOfWorkers 3 --executor-per-worker 1 --duration 70000 --task-max-execute-time 1000";
      shell.execute(command);

  }
  

}