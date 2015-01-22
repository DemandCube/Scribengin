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
  
  public YarnVMClient(Registry registry, Map<String, String> yarnProps, Configuration conf) {
    super(registry);
    this.yarnProps = yarnProps;
    this.conf = conf ;
  }
  
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
    appClient.run(vmConfig, yarnConf);
  }
  
  public void configureEnvironment(VMConfig vmConfig) {
    vmConfig.setEnvironment(VMConfig.Environment.YARN_MINICLUSTER);
    vmConfig.getYarnConf().putAll(yarnProps);
  }
}
