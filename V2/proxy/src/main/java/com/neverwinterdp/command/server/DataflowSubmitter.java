package com.neverwinterdp.command.server;

import javax.servlet.http.HttpServletRequest;

import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.command.Command;

public class DataflowSubmitter  { 
  protected ScribenginClient scribeClient;
  
  public DataflowSubmitter(ScribenginClient client){
    this.scribeClient = client;
  }
  
  
  public ScribenginWaitingEventListener submit(HttpServletRequest request) throws Exception {
    DataflowDescriptor descriptor = DescriptorBuilder.parseDataflowInput(request);
    System.out.println("Submit the dataflow " + descriptor.getName());
    String name = descriptor.getName() ;
    ScribenginWaitingEventListener waitingEventListener = new ScribenginWaitingEventListener(scribeClient.getRegistry());
    waitingEventListener.waitDataflowLeader(format("Expect %s-master-1 as the leader", name), name,  format("%s-master-1", name));
    waitingEventListener.waitDataflowStatus("Expect dataflow init status", name, DataflowLifecycleStatus.INIT);
    waitingEventListener.waitDataflowStatus("Expect dataflow running status", name, DataflowLifecycleStatus.RUNNING);
    waitingEventListener.waitDataflowStatus("Expect dataflow  finish status", name, DataflowLifecycleStatus.FINISH);
    
    VMDescriptor scribenginMaster = scribeClient.getScribenginMaster();
    
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(descriptor) ;
    //CommandResult<Boolean> result = 
    //    (CommandResult<Boolean>)scribeClient.execute(scribenginMaster, deployCmd, 35000);
    scribeClient.execute(scribenginMaster, deployCmd, 35000);
    return waitingEventListener ;
  }
  
  
  private String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
  }

}
