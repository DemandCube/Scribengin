package com.neverwinterdp.scribengin.dataflow;

import static com.neverwinterdp.vm.builder.VMClusterBuilder.h1;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

public class DataflowClient {
  protected ScribenginClient scribenginClient ;
  
  public DataflowClient(ScribenginClient scribenginClient) {
    this.scribenginClient = scribenginClient;
  }
  
  public ScribenginWaitingEventListener submit(String dataflowAppHome, String jsonDescriptor) throws Exception {
    DataflowDescriptor descriptor = JSONSerializer.INSTANCE.fromString(jsonDescriptor, DataflowDescriptor.class) ;
    return submit(dataflowAppHome, descriptor) ;
  }
  
  public ScribenginWaitingEventListener submit(DataflowDescriptor descriptor) throws Exception {
    return submit(null, descriptor) ;
  }
  
  public ScribenginWaitingEventListener submit(String localDataflowHome, DataflowDescriptor descriptor) throws Exception {
    if(localDataflowHome != null) {
      VMDescriptor vmMaster = scribenginClient.getVMClient().getMasterVMDescriptor();
      VMConfig vmConfig = vmMaster.getVmConfig();
      String dataflowAppHome = vmConfig.getAppHome() + "/dataflows/" + descriptor.getName();
      descriptor.setDataflowAppHome(dataflowAppHome);
      scribenginClient.getVMClient().uploadApp(localDataflowHome, dataflowAppHome);
    }
    h1("Submit the dataflow " + descriptor.getName());
    String name = descriptor.getName() ;
    VMClient vmClient = new VMClient(scribenginClient.getRegistry());
    ScribenginWaitingEventListener waitingEventListener = new ScribenginWaitingEventListener(vmClient.getRegistry());
    waitingEventListener.waitDataflowLeader(format("Expect %s-master-1 as the leader", name), name,  format("%s-master-1", name));
    waitingEventListener.waitDataflowStatus("Expect dataflow init status", name, DataflowLifecycleStatus.INIT);
    waitingEventListener.waitDataflowStatus("Expect dataflow running status", name, DataflowLifecycleStatus.RUNNING);
    waitingEventListener.waitDataflowStatus("Expect dataflow  finish status", name, DataflowLifecycleStatus.FINISH);
   
    VMDescriptor scribenginMaster = scribenginClient.getScribenginMaster();
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(descriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    return waitingEventListener ;
  }
  
  private String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }
}
