package com.neverwinterdp.scribengin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;

import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMServicePlugin;
import com.neverwinterdp.vm.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.master.VMManagerApp;

public class VMScribenginSingleJVMUnitTest extends VMScribenginUnitTest {
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    super.setup();
  }
  
  protected void createVMMaster(String name) throws Exception {
    String[] args = {
      "--environment", "JVM",
      "--name", name,
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMManagerApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + JVMVMServicePlugin.class.getName()
    };
    VM vm = VM.run(args);
  }
  
  protected void configureEnvironment(VMConfig vmConfig) {
    vmConfig.setEnvironment(VMConfig.Environment.JVM);
  }

  @Override
  protected FileSystem getFileSystem() throws Exception {
    return FileSystem.get(new Configuration());
  }

  @Override
  protected String getDataDir() { return "./build/hdfs"; }
}