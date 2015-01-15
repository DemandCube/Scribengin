package com.neverwinterdp.scribengin.service;

import java.util.Arrays;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.service.VMDataflowServiceApp;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.service.VMServiceCommand;

@Singleton
public class ScribenginService {
  final static public String SCRIBENGIN_PATH = "/scribengin";
  final static public String LEADER_PATH     = "/scribengin/master/leader";
  final static public String DATAFLOWS_PATH  = "/scribengin/dataflows";
  
  @Inject
  private VMConfig vmConfig; 
  private Registry registry;
  private VMClient vmClient ;
  
  @Inject
  public void onInit(Registry registry, VMClient vmClient) throws Exception {
    this.registry = registry;
    registry.createIfNotExist(DATAFLOWS_PATH);
    this.vmClient = vmClient;
  }
  
  public void onDestroy() throws Exception {
  }
  
  public boolean deploy(DataflowDescriptor descriptor) throws Exception {
    String dataflowPath = DATAFLOWS_PATH + "/" + descriptor.getName();
    registry.create(dataflowPath, descriptor, NodeCreateMode.PERSISTENT);
    registry.createIfNotExist(dataflowPath + "/master/leader");
    DataflowDeployer deployer = new DataflowDeployer(descriptor);
    deployer.start();
    return true;
  }
  
  private VMDescriptor createDataflowMaster(DataflowDescriptor descriptor, int id) throws Exception {
    String dataflowPath = DATAFLOWS_PATH + "/" + descriptor.getName();
    VMConfig dfVMConfig = new VMConfig() ;
    dfVMConfig.setEnvironment(vmConfig.getEnvironment());
    dfVMConfig.setName(descriptor.getName() + "-master-" + id);
    dfVMConfig.setRoles(Arrays.asList("dataflow-master"));
    dfVMConfig.setRegistryConfig(registry.getRegistryConfig());
    dfVMConfig.setVmApplication(VMDataflowServiceApp.class.getName());
    dfVMConfig.addProperty("dataflow.registry.path", dataflowPath);
    dfVMConfig.setYarnConf(vmConfig.getYarnConf());
    System.out.println("VMConfig dfVMConfig");
    System.out.println(JSONSerializer.INSTANCE.toString(dfVMConfig));
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    CommandResult<VMDescriptor> result = 
        (CommandResult<VMDescriptor>)vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(dfVMConfig));
    System.out.println(result.getErrorStacktrace());
    return result.getResult();
  }
  
  public class DataflowDeployer extends Thread {
    private DataflowDescriptor descriptor;
    
    public DataflowDeployer(DataflowDescriptor descriptor) {
      this.descriptor = descriptor;
    }
    
    public void run() {
      try {
        VMDescriptor master1 = createDataflowMaster(descriptor, 1);
        //VMDescriptor master2 = createDataflowMaster(descriptor, 2);
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
