package com.neverwinterdp.scribengin.dataflow.builder;

import static com.neverwinterdp.vm.builder.VMClusterBuilder.*;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

abstract public class DataflowBuilder {
  protected ScribenginClusterBuilder clusterBuilder ;
  
  public DataflowBuilder(ScribenginClusterBuilder clusterBuilder) {
    this.clusterBuilder = clusterBuilder;
  }
  
  public ScribenginWaitingEventListener submit() throws Exception {
    DataflowDescriptor descriptor = createDataflowDescriptor();
    h1("Submit the dataflow " + descriptor.getName());
    String name = descriptor.getName() ;
    VMClient vmClient = clusterBuilder.getVMClusterBuilder().getVMClient();
    ScribenginWaitingEventListener waitingEventListener = new ScribenginWaitingEventListener(vmClient.getRegistry());
    waitingEventListener.waitDataflowLeader(format("Expect %s-master-1 as the leader", name), name,  format("%s-master-1", name));
    waitingEventListener.waitDataflowStatus("Expect dataflow init status", name, DataflowLifecycleStatus.INIT);
    waitingEventListener.waitDataflowStatus("Expect dataflow running status", name, DataflowLifecycleStatus.RUNNING);
    waitingEventListener.waitDataflowStatus("Expect dataflow  finish status", name, DataflowLifecycleStatus.FINISH);
    VMDescriptor scribenginMaster = clusterBuilder.getScribenginClient().getScribenginMaster();
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(descriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    return waitingEventListener ;
  }
  
  abstract protected DataflowDescriptor createDataflowDescriptor() ;
  
  private String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }
}
