package com.neverwinterdp.vm.client;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.environment.yarn.AppClient;
import com.neverwinterdp.vm.environment.yarn.YarnVMServicePlugin;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;

public class YarnVMClient extends VMClient {
  private Map<String, String> yarnProps ;
  private Configuration conf ;
  private VMConfig.Environment yarnEnv = VMConfig.Environment.YARN_MINICLUSTER ;
  private String localAppHome = ".";
  private String dfsAppHome = "/apps/VMApp" ;
  
  public YarnVMClient(Registry registry, VMConfig.Environment yarnEnv, Map<String, String> yarnProps) {
    super(registry);
    this.yarnEnv = yarnEnv;
    this.yarnProps = yarnProps;
    conf = new Configuration() ;
    for(Map.Entry<String, String> entry : yarnProps.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }
  
  public YarnVMClient(Registry registry, Map<String, String> yarnProps, Configuration conf) {
    super(registry);
    this.yarnProps = yarnProps;
    this.conf = conf ;
  }
  
  public void setDFSAppHome(String home) { this.dfsAppHome = home; }
  
  public void setLocalAppHome(String home) { this.localAppHome = home; }
  
  @Override
  public void createVMMaster(String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.setName(name);
    vmConfig.addRoles("vm-master") ;
    vmConfig.setSelfRegistration(true) ;
    vmConfig.setRegistryConfig(getRegistry().getRegistryConfig());
    vmConfig.setVmApplication(VMServiceApp.class.getName()) ;
    vmConfig.addProperty("implementation:" + VMServicePlugin.class.getName(), YarnVMServicePlugin.class.getName()) ;
    configureEnvironment(vmConfig);

    AppClient appClient = new AppClient() ;
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    for(Map.Entry<String, String> entry : yarnProps.entrySet()) {
      yarnConf.set(entry.getKey(), entry.getValue());
    }
    appClient.uploadApp(vmConfig, localAppHome, dfsAppHome);
    appClient.run(vmConfig, yarnConf);
  }
  
  public void configureEnvironment(VMConfig vmConfig) {
    vmConfig.setEnvironment(yarnEnv);
    vmConfig.getYarnConf().putAll(yarnProps);
  }
}
