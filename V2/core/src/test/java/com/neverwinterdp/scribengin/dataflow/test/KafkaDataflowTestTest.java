package com.neverwinterdp.scribengin.dataflow.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;

public class KafkaDataflowTestTest {
  
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
    try{
      clusterBuilder.shutdown();
    } catch(Exception e){}
    
  }
  
  
  @Test
  public void testKafkaDataflowTest() throws Exception{
    shell.execute("dataflow-test kafka");
  }
  
  protected static VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }
}
