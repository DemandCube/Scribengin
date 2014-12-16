package com.neverwinterdp.scribengin;

import com.beust.jcommander.JCommander;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.master.VMDataflowMasterApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.master.command.VMMasterCommand;

@Singleton
public class ScribenginMaster {
  final static public String SCRIBENGIN_PATH = "/scribengin";
  final static public String LEADER_PATH     = "/scribengin/master/leader";
  final static public String DATAFLOWS_PATH  = "/scribengin/dataflows";
  
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
    RegistryConfig config = registry.getRegistryConfig();
    String[] args = {
        "--name", descriptor.getName() + "-master-" + id,
        "--roles", "dataflow-master",
        "--registry-connect", config.getConnect(), 
        "--registry-db-domain", config.getDbDomain(), 
        "--registry-implementation", config.getRegistryImplementation(),
        "--vm-application", VMDataflowMasterApp.class.getName(),
        "--prop:dataflow.registry.path=" + dataflowPath
    };
    VMConfig vmConfig = new VMConfig() ;
    new JCommander(vmConfig, args);
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    CommandResult<VMDescriptor> result = 
        (CommandResult<VMDescriptor>)vmClient.execute(masterVMDescriptor, new VMMasterCommand.Allocate(vmConfig));
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
        VMDescriptor master2 = createDataflowMaster(descriptor, 2);
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
