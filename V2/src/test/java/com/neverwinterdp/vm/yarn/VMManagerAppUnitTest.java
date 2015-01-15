package com.neverwinterdp.vm.yarn;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.hadoop.MiniClusterUtil;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VMDummyApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.environment.yarn.AppClient;
import com.neverwinterdp.vm.environment.yarn.YarnVMServicePlugin;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServiceCommand;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class VMManagerAppUnitTest {
  
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties") ;
  }
  
  protected ZookeeperServerLauncher zkServerLauncher ;
  
  MiniYARNCluster miniYarnCluster ;

  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new ZookeeperServerLauncher("./build/data/zookeeper") ;
    zkServerLauncher.start();
    
    YarnConfiguration yarnConf = new YarnConfiguration() ;
    yarnConf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");
    miniYarnCluster = MiniClusterUtil.createMiniYARNCluster(yarnConf, 1);
    Configuration conf = miniYarnCluster.getConfig() ;
//    Iterator<Map.Entry<String, String>> i = conf.iterator();
//    while(i.hasNext()) {
//      Map.Entry<String, String> entry =  i.next();
//      System.out.println(entry.getKey() + " = " + entry.getValue());
//    }
  }

  @After
  public void teardown() throws Exception {
    miniYarnCluster.stop();
    miniYarnCluster.close();
    zkServerLauncher.shutdown();
  }

  @Test
  public void testAppClient() throws Exception {
    String[] args = createVMConfigArgs("vm-master-1");
    AppClient appClient = new AppClient() ;
    appClient.run(args, new YarnConfiguration(miniYarnCluster.getConfig()));
    Thread.sleep(10000);

    Shell shell = new Shell(new RegistryImpl(RegistryConfig.getDefault()).connect()) ;
    VMClient vmClient = shell.getVMClient();
    
    shell.execute("vm list");
    VMDescriptor vmMaster1 = shell.getVMClient().getMasterVMDescriptor();
    
    VMDescriptor vmDummy1 = allocateVMDummy(vmClient, "vm-dummy-1") ;
    shell.execute("vm list");
    Thread.sleep(5000);

    VMDescriptor vmDummy2 = allocateVMDummy(vmClient, "vm-dummy-2") ;
    shell.execute("vm list");
    Thread.sleep(5000);
    
    shutdown(vmClient, vmDummy1);
    shutdown(vmClient, vmDummy2);
    Thread.sleep(1000);
    shell.execute("vm list");
    shell.execute("registry dump");
    shutdown(vmClient, vmMaster1);
    Thread.sleep(1000);
    shell.execute("vm list");
    shell.execute("vm history");
  }
  
  private String[] createVMConfigArgs(String name) {
    String[] args = { 
        "--environment", "YARN_MINICLUSTER",
        "--name", name,
        "--roles", "vm-master",
        "--self-registration",
        "--registry-connect", "127.0.0.1:2181", 
        "--registry-db-domain", "/NeverwinterDP", 
        "--registry-implementation", RegistryImpl.class.getName(),
        "--vm-application",VMServiceApp.class.getName(),
        "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + YarnVMServicePlugin.class.getName(),
        "--yarn:yarn.resourcemanager.scheduler.address=0.0.0.0:8030"
    } ;
    return args;
  }
  
  private VMDescriptor allocateVMMaster(VMClient vmClient, String name) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    VMConfig vmConfig = new VMConfig() ;
    String[] args = createVMConfigArgs(name);
    new JCommander(vmConfig, args);
    Configuration conf = miniYarnCluster.getConfig() ;
    Iterator<Map.Entry<String, String>> i = conf.iterator();
    while(i.hasNext()) {
      Map.Entry<String, String> entry =  i.next();
      //vmConfig.getYarnConf().put(entry.getKey(), entry.getValue());
    }
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(vmConfig));
    Assert.assertNull(result.getErrorStacktrace());
    VMDescriptor vmDescriptor = result.getResultAs(VMDescriptor.class);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  private VMDescriptor allocateVMDummy(VMClient vmClient, String name) throws Exception {
    String[] args = { 
        "--environment", "YARN_MINICLUSTER",
        "--name", name,
        "--roles", "dummy",
        "--self-registration",
        "--registry-connect", "127.0.0.1:2181", 
        "--registry-db-domain", "/NeverwinterDP", 
        "--registry-implementation", RegistryImpl.class.getName(),
        "--vm-application", VMDummyApp.class.getName(),
        "--yarn:yarn.resourcemanager.scheduler.address=0.0.0.0:8030"
    } ;
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    VMConfig vmConfig = new VMConfig() ;
    new JCommander(vmConfig, args);
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(vmConfig));
    Assert.assertNull(result.getErrorStacktrace());
    VMDescriptor vmDescriptor = result.getResultAs(VMDescriptor.class);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  private boolean shutdown(VMClient vmClient, VMDescriptor vmDescriptor) throws Exception {
    CommandResult<?> result = vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    Assert.assertNull(result.getErrorStacktrace());
    return result.getResultAs(Boolean.class);
  }
}