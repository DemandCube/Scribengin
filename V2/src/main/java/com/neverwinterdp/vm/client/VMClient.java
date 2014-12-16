package com.neverwinterdp.vm.client;

import java.util.List;

import org.junit.Assert;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandPayload;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.master.command.VMMasterCommand;

public class VMClient {
  private Registry registry;

  public VMClient(Registry registry) {
    this.registry = registry;
  }
  
  public Registry getRegistry() { return this.registry ; }
  
  public List<VMDescriptor> getRunningVMDescriptors() throws RegistryException {
    return registry.getChildrenAs(VMService.ALLOCATED_PATH, VMDescriptor.class) ;
  }
  
  public List<VMDescriptor> getHistoryVMDescriptors() throws RegistryException {
    return registry.getChildrenAs(VMService.HISTORY_PATH, VMDescriptor.class) ;
  }
  
  public VMDescriptor getMasterVMDescriptor() throws RegistryException { 
    VMDescriptor descriptor = registry.getDataAs(VMService.LEADER_PATH, VMDescriptor.class);
    return descriptor;
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command) throws RegistryException, Exception {
    return execute(vmDescriptor, command, 30000);
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command, long timeout) throws RegistryException, Exception {
    CommandPayload payload = new CommandPayload(command, null) ;
    Node node = registry.create(vmDescriptor.getStoredPath() + "/commands/command-", payload, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    CommandReponseWatcher responseWatcher = new CommandReponseWatcher();
    node.watch(responseWatcher);
    return responseWatcher.waitForResult(timeout);
  }
  
  public void execute(VMDescriptor vmDescriptor, Command command, CommandCallback callback) {
  }
  
  public VMDescriptor allocate(VMConfig vmConfig) throws Exception {
    VMDescriptor masterVMDescriptor = getMasterVMDescriptor();
    CommandResult<VMDescriptor> result = 
        (CommandResult<VMDescriptor>) execute(masterVMDescriptor, new VMMasterCommand.Allocate(vmConfig));
    if(result.getErrorStacktrace() != null) {
      throw new Exception(result.getErrorStacktrace());
    }
    return result.getResult();
  }
  
  public boolean shutdown(VMDescriptor vmDescriptor) throws Exception {
    CommandResult<?> result = execute(vmDescriptor, new VMCommand.Shutdown());
    return result.getResultAs(Boolean.class);
  }
  
  public class CommandReponseWatcher implements NodeWatcher {
    private CommandResult<?> result ;
    private Exception error ;
    
    @Override
    public void process(NodeEvent event) {
      String path = event.getPath();
      try {
        CommandPayload payload = registry.getDataAs(path, CommandPayload.class) ;
        result = payload.getResult() ;
        registry.delete(path);
        synchronized(this) {
          notify();
        }
      } catch(Exception e) {
        error = e ;
      }
    }
    
    public CommandResult<?> waitForResult(long timeout) throws Exception {
      if(result == null) {
        synchronized(this) {
          wait(timeout);
        }
      }
      if(error != null) throw error;
      return result ;
    }
  }
}
