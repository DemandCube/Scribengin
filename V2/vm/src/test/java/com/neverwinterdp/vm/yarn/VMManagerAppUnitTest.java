package com.neverwinterdp.vm.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.hadoop.MiniClusterUtil;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMDummyApp;
import com.neverwinterdp.vm.builder.EmbededVMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;
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
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
 
  EmbededVMClusterBuilder vmCluster ;
  MiniYARNCluster miniYarnCluster ;

  @Before
  public void setup() throws Exception {
    YarnConfiguration yarnConf = new YarnConfiguration() ;
    yarnConf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");
    miniYarnCluster = MiniClusterUtil.createMiniYARNCluster(yarnConf, 1);
    Configuration conf = miniYarnCluster.getConfig() ;
    
    HadoopProperties yarnProps = new HadoopProperties();
    yarnProps.put("yarn.resourcemanager.scheduler.address", "0.0.0.0:8030");
    Registry registry = new RegistryImpl(RegistryConfig.getDefault());
    YarnVMClient vmClient = new YarnVMClient(registry, yarnProps,miniYarnCluster.getConfig());
    vmCluster = new EmbededVMClusterBuilder(vmClient) ;
    vmCluster.clean(); 
    vmCluster.startKafkaCluster();
    vmCluster.getVMClient().getRegistry().connect();
  }

  @After
  public void teardown() throws Exception {
    miniYarnCluster.stop();
    miniYarnCluster.close();
    vmCluster.shutdown();
  }

  @Test
  public void testAppClient() throws Exception {
    VMClient vmClient = vmCluster.getVMClient();
    Shell shell = new Shell(vmClient) ;
    
    String[] args = createVMConfigArgs("vm-master-1");
    VMConfig vmConfig = new VMConfig() ;
    new JCommander(vmConfig, args) ;
    AppClient appClient = new AppClient(vmConfig.getHadoopProperties()) ;
    appClient.run(vmConfig, new YarnConfiguration(miniYarnCluster.getConfig()));
    Thread.sleep(10000);
    
    shell.execute("vm info");
    VMDescriptor vmMaster1 = shell.getVMClient().getMasterVMDescriptor();
    
    VMDescriptor vmDummy1 = allocateVMDummy(vmClient, "vm-dummy-1") ;
    shell.execute("vm info");
    Thread.sleep(5000);

    VMDescriptor vmDummy2 = allocateVMDummy(vmClient, "vm-dummy-2") ;
    shell.execute("vm info");
    Thread.sleep(5000);
    
    shutdown(vmClient, vmDummy1);
    shutdown(vmClient, vmDummy2);
    Thread.sleep(1000);
    shell.execute("vm info");
    shell.execute("registry dump");
    shutdown(vmClient, vmMaster1);
    Thread.sleep(1000);
    shell.execute("vm info");
  }
  
  private String[] createVMConfigArgs(String name) {
    String[] args = { 
        "--environment", "YARN_MINICLUSTER",
        "--name", name,
        "--role", "vm-master",
        "--self-registration",
        "--registry-connect", "127.0.0.1:2181", 
        "--registry-db-domain", "/NeverwinterDP", 
        "--registry-implementation", RegistryImpl.class.getName(),
        "--vm-application",VMServiceApp.class.getName(),
        "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + YarnVMServicePlugin.class.getName(),
        "--hadoop:yarn.resourcemanager.scheduler.address=0.0.0.0:8030"
    } ;
    return args;
  }
  
  private VMDescriptor allocateVMDummy(VMClient vmClient, String name) throws Exception {
    String[] args = { 
        "--environment", "YARN_MINICLUSTER",
        "--name", name,
        "--role", "dummy",
        "--self-registration",
        "--registry-connect", "127.0.0.1:2181", 
        "--registry-db-domain", "/NeverwinterDP", 
        "--registry-implementation", RegistryImpl.class.getName(),
        "--vm-application", VMDummyApp.class.getName(),
        "--hadoop:yarn.resourcemanager.scheduler.address=0.0.0.0:8030"
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