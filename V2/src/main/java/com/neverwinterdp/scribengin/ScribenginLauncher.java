package com.neverwinterdp.scribengin;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.service.VMScribenginServiceApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.environment.yarn.AppClient;
import com.neverwinterdp.vm.environment.yarn.YarnVMServicePlugin;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class ScribenginLauncher {
  private long vmLaunchTime = 30 * 1000; //30s
  
  private ScribenginShell shell;
  private VMClient vmClient;
  
  void createVMMaster() throws Exception {
    String[] runArgs = {
      "--environment", "YARN",
      "--local-home", ".",
      "--dfs-home", "/apps/scribengin.v2",
      "--name", "VMMaster",
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "zookeeper:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMServiceApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + YarnVMServicePlugin.class.getName(),
      "--yarn:yarn.resourcemanager.scheduler.address=hadoop-master:8030",
      "--yarn:yarn.resourcemanager.address=hadoop-master:8032",
      "--yarn:fs.defaultFS=hdfs://hadoop-master:9000",
      //"--yarn:fs.default.name=hdfs://hadoop-master:9000",
    };
    AppClient appClient = new AppClient() ;
    appClient.run(runArgs, new YarnConfiguration());
  }
  
  Registry newRegistry() {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("zookeeper:2181");
    config.setDbDomain("/NeverwinterDP");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return new RegistryImpl(config);
  }
 
  
  public void run() throws Exception {
    Registry registry = newRegistry().connect();
    shell = new ScribenginShell(registry);
    vmClient = shell.getVMClient();
    shell.execute("registry dump --path /");
    
    banner("Create VM Master");
    createVMMaster();
    Thread.sleep(vmLaunchTime);
    
    banner("Create Scribengin Master 1");
    VMDescriptor scribenginMaster1 = createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    Thread.sleep(vmLaunchTime);
    
    banner("Create Scribengin Master 2");
    VMDescriptor scribenginMaster2 = createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    Thread.sleep(vmLaunchTime);

    shell.execute("vm list");
    shell.execute("registry dump --path /");
  }

  private void banner(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  protected VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setEnvironment(VMConfig.Environment.YARN).
      setName(name).
      addRoles("scribengin-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMScribenginServiceApp.class.getName());
    vmConfig.
      addYarnProperty("yarn.resourcemanager.scheduler.address", "localhost:8030").
      addYarnProperty("fs.defaultFS", "hdfs://hadoop-master:9000");
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
    return vmDescriptor;
  }
  
  static public void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "neverwinterdp"); 
    new ScribenginLauncher().run();
  }
}
