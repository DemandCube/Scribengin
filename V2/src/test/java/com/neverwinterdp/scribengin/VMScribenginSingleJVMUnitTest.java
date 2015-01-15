package com.neverwinterdp.scribengin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.environment.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class VMScribenginSingleJVMUnitTest extends VMScribenginUnitTest {
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    super.setup();
  }
  
  protected void createVMMaster(String name) throws Exception {
    RegistryConfig rConfig = new RegistryConfig();
    rConfig.setConnect("127.0.0.1:2181");
    rConfig.setDbDomain("/NeverwinterDP");
    rConfig.setRegistryImplementation(RegistryImpl.class.getName());
    
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setEnvironment(VMConfig.Environment.JVM).
      setName(name).
      addRoles("vm-master").
      setSelfRegistration(true).
      setVmApplication(VMServiceApp.class.getName()).
      addProperty("implementation:" + VMServicePlugin.class.getName(), JVMVMServicePlugin.class.getName()).
      setRegistryConfig(rConfig);
    VM vm = VM.run(vmConfig);
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