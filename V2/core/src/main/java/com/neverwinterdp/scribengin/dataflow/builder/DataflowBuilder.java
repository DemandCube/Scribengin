package com.neverwinterdp.scribengin.dataflow.builder;

import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.event.ScribenginAssertEventListener;
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
  
  public ScribenginAssertEventListener submit() throws Exception {
    DataflowDescriptor descriptor = createDataflowDescriptor();
    String name = descriptor.getName() ;
    VMClient vmClient = clusterBuilder.getVMClusterBuilder().getVMClient();
    ScribenginAssertEventListener scribenginAssert = new ScribenginAssertEventListener(vmClient.getRegistry());
    scribenginAssert.watchDataflow(name);
    scribenginAssert.assertDataflowMaster(format("Expect %s-master-1 as the leader", name), format("%s-master-1", name));
    scribenginAssert.assertDataflowStatus("Expect dataflow init status", name, DataflowLifecycleStatus.INIT);
    scribenginAssert.assertDataflowStatus("Expect dataflow running status", name, DataflowLifecycleStatus.RUNNING);
    scribenginAssert.assertDataflowStatus("Expect dataflow  finish status", name, DataflowLifecycleStatus.FINISH);
    VMDescriptor scribenginMaster = clusterBuilder.getScribenginClient().getScribenginMaster();
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(descriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    return scribenginAssert ;
  }
  
  abstract protected DataflowDescriptor createDataflowDescriptor() ;
  
  private String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }
}
