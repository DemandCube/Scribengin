package com.neverwinterdp.vm.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.jcraft.jsch.Logger;
import com.neverwinterdp.hadoop.MiniClusterUtil;
import com.neverwinterdp.hadoop.yarn.app.AppClient;
import com.neverwinterdp.hadoop.yarn.app.AppClientMonitor;
import com.neverwinterdp.hadoop.yarn.app.protocol.IPCService;
import com.neverwinterdp.hadoop.yarn.app.protocol.Void;
import com.neverwinterdp.netty.rpc.client.DefaultClientRPCController;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.dependency.ZookeeperServerLauncher;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.master.command.VMMasterCommand;
import com.neverwinterdp.vm.yarn.VMContainerManager;

public class VMManagerApplicationUnitTest {
  
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
  }

  @After
  public void teardown() throws Exception {
    miniYarnCluster.stop();
    miniYarnCluster.close();
    zkServerLauncher.stop();
  }

  @Test
  public void testAppClient() throws Exception {
    String[] args = { 
      "--mini-cluster-env",
      "--app-name", "VMCluster",
      "--app-container-manager", VMContainerManager.class.getName(),
      "--app-rpc-port", "63200" ,
      //"--app-history-server-address", "http://127.0.0.1:9090/yarn-app/history",
      "--conf:yarn.resourcemanager.scheduler.address=0.0.0.0:8030"
    } ;
    AppClient appClient = new AppClient() ;
    AppClientMonitor reporter = 
        appClient.run(args, new YarnConfiguration(miniYarnCluster.getConfig()));
    
    IPCService.BlockingInterface ipcService = reporter.getIPCService() ;
    System.out.println("Status: " + ipcService.getAppMasterStatus(new DefaultClientRPCController(), Void.getDefaultInstance())) ;
    Thread.sleep(3000);
    Shell shell = new Shell(new RegistryImpl(RegistryConfig.getDefault()).connect()) ;
    shell.execute("vm list");
    
    VMClient vmClient = shell.getVMClient();
    VMDescriptor sMaster1 = allocateVMMaster(vmClient, "vm-master-2") ;
    shell.execute("vm list");
    
    reporter.monitor(); 
    reporter.report(System.out);
  }
  
  private VMDescriptor allocateVMMaster(VMClient vmClient, String name) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    VMConfig scribenginMasterVMConfig = new VMConfig() ;
    scribenginMasterVMConfig.setName(name);
    scribenginMasterVMConfig.setRoles(new String[] {"vm-master"});
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMMasterCommand.Allocate(scribenginMasterVMConfig));
    Assert.assertNull(result.getErrorStacktrace());
    VMDescriptor vmDescriptor = result.getResultAs(VMDescriptor.class);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
}