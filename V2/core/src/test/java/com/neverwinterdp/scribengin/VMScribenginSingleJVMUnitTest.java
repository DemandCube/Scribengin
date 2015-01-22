package com.neverwinterdp.scribengin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;

import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;

public class VMScribenginSingleJVMUnitTest extends VMScribenginUnitTest {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    vmLaunchTime = 100;
    super.setup();
  }
  
  @Override
  protected FileSystem getFileSystem() throws Exception {
    return FileSystem.get(new Configuration());
  }

  @Override
  protected String getDataDir() { return "./build/hdfs"; }
  
  protected VMClient getVMClient() throws Exception {
    LocalVMClient vmClient = new LocalVMClient();
    return vmClient;
  }
  
  protected VMClusterBuilder getVMClusterBuilder() throws Exception {
    EmbededVMClusterBuilder builder = new EmbededVMClusterBuilder();
    return builder;
  }
}