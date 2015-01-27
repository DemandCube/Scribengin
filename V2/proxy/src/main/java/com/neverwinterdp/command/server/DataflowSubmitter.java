package com.neverwinterdp.command.server;

import javax.servlet.http.HttpServletRequest;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

public class DataflowSubmitter  {
  protected VMClient vmClient; 
  
  public DataflowSubmitter(VMClient client){
    this.vmClient = client;
  }
  
  
  public ScribenginWaitingEventListener submit(HttpServletRequest request) throws Exception {
    DataflowDescriptor descriptor = DescriptorBuilder.parseDataflowInput(request);
    System.out.println("Submit the dataflow " + descriptor.getName());
    String name = descriptor.getName() ;
    ScribenginWaitingEventListener waitingEventListener = new ScribenginWaitingEventListener(vmClient.getRegistry());
    waitingEventListener.waitDataflowLeader(format("Expect %s-master-1 as the leader", name), name,  format("%s-master-1", name));
    waitingEventListener.waitDataflowStatus("Expect dataflow init status", name, DataflowLifecycleStatus.INIT);
    waitingEventListener.waitDataflowStatus("Expect dataflow running status", name, DataflowLifecycleStatus.RUNNING);
    waitingEventListener.waitDataflowStatus("Expect dataflow  finish status", name, DataflowLifecycleStatus.FINISH);
    
    VMDescriptor scribenginMaster = vmClient.getMasterVMDescriptor();
    
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(descriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    return waitingEventListener ;
  }
  
  
  private String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }

}
