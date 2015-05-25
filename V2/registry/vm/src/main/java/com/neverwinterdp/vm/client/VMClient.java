package com.neverwinterdp.vm.client;

import static com.neverwinterdp.vm.tool.VMClusterBuilder.h1;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandPayload;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.service.VMService;
import com.neverwinterdp.vm.service.VMServiceCommand;

@JmxBean("role=vm-client, type=VMClient, name=VMClient")
public class VMClient {
  private Registry registry;
  private long     waitForResultTimeout = 30000;

  public VMClient(Registry registry) {
    this.registry = registry;
  }
  
  public Registry getRegistry() { return this.registry ; }

  public long getWaitForResultTimeout() { return waitForResultTimeout; }

  public void setWaitForResultTimeout(long waitForResultTimeout) {
    this.waitForResultTimeout = waitForResultTimeout;
  }

  public List<VMDescriptor> getActiveVMDescriptors() throws RegistryException {
    return VMService.getActiveVMDescriptors(registry) ;
  }
  
  public List<VMDescriptor> getHistoryVMDescriptors() throws RegistryException {
    return VMService.getHistoryVMDescriptors(registry) ;
  }
  public List<VMDescriptor> getAllVMDescriptors() throws RegistryException {
    return VMService.getAllVMDescriptors(registry) ;
  }
  
  public VMDescriptor getMasterVMDescriptor() throws RegistryException { 
    Node vmNode = registry.getRef(VMService.LEADER_PATH);
    return vmNode.getDataAs(VMDescriptor.class);
  }
  
  public void shutdown() throws Exception {
    h1("Shutdow the vm masters");
    registry.create(VMService.SHUTDOWN_EVENT_PATH, true, NodeCreateMode.PERSISTENT);
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command) throws RegistryException, Exception {
    return execute(vmDescriptor, command, waitForResultTimeout);
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command, long timeout) throws Exception {
    CommandPayload payload = new CommandPayload(command, null) ;
    Node node = registry.create(vmDescriptor.getRegistryPath() + "/commands/command-", payload, NodeCreateMode.PERSISTENT_SEQUENTIAL);
    CommandReponseWatcher responseWatcher = new CommandReponseWatcher(registry, node.getPath(), command);
    node.watchModify(responseWatcher);
    return responseWatcher.waitForResult(timeout);
  }
  
  public void execute(VMDescriptor vmDescriptor, Command command, CommandCallback callback) {
  }
  
  public VMDescriptor allocate(VMConfig vmConfig) throws Exception {
    VMDescriptor masterVMDescriptor = getMasterVMDescriptor();
    CommandResult<VMDescriptor> result = 
        (CommandResult<VMDescriptor>) execute(masterVMDescriptor, new VMServiceCommand.Allocate(vmConfig));
    if(result.getErrorStacktrace() != null) {
      registry.get("/").dump(System.err);
      throw new Exception(result.getErrorStacktrace());
    }
    return result.getResult();
  }
  
  public VMDescriptor allocate(String localAppHome, VMConfig vmConfig) throws Exception {
    return allocate(vmConfig);
  }
  
  public boolean shutdown(VMDescriptor vmDescriptor) throws Exception {
    CommandResult<?> result = execute(vmDescriptor, new VMCommand.Shutdown());
    if(result.isDiscardResult()) return true;
    return result.getResultAs(Boolean.class);
  }
  
  public boolean simulateKill(VMDescriptor vmDescriptor) throws Exception {
    CommandResult<?> result = execute(vmDescriptor, new VMCommand.SimulateKill());
    if(result.isDiscardResult()) return true;
    return result.getResultAs(Boolean.class);
  }
  
  public boolean kill(VMDescriptor vmDescriptor) throws Exception {
    //CommandResult<?> result = execute(vmDescriptor, new VMCommand.Kill());
    
    Command command = new VMCommand.Kill();
    CommandPayload payload = new CommandPayload(command, null) ;
    Node node = registry.create(vmDescriptor.getRegistryPath() + "/commands/command-", payload, NodeCreateMode.PERSISTENT_SEQUENTIAL);
    CommandReponseWatcher responseWatcher = new CommandReponseWatcher(registry, node.getPath(), command);
    node.watchModify(responseWatcher);
    
    try {
      CommandResult<?> result = responseWatcher.waitForResult(60000);
      if(result.isDiscardResult()) return true;
      return result.getResultAs(Boolean.class);
    } catch(Exception ex) {
      throw ex ;
    }
  }
  
  public boolean kill(VMDescriptor vmDescriptor, long timeout) throws Exception {
    CommandResult<?> result = execute(vmDescriptor, new VMCommand.Kill(), timeout);
    if(result.isDiscardResult()) return true;
    return result.getResultAs(Boolean.class);
  }
  
  
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(new Configuration());
  }
  
  public void uploadApp(String localAppHome, String appHome) throws Exception {
  }
  
  public void createVMMaster(String name) throws Exception {
    throw new RuntimeException("This method need to override") ;
  }
  
  public void configureEnvironment(VMConfig vmConfig) {
    throw new RuntimeException("This method need to override") ;
  }
  
  static public class CommandReponseWatcher extends NodeWatcher {
    private Registry         registry;
    private String           path;
    private Command          command ;
    private CommandResult<?> result;
    private Exception        error;
    private boolean          discardResult = false;
    
    public CommandReponseWatcher(Registry registry, String path, Command command) {
      this.registry = registry;
      this.path = path;
      this.command = command ;
    }
    
    @Override
    public void onEvent(NodeEvent event) {
      String path = event.getPath();
      try {
        if(event.getType() == NodeEvent.Type.DELETE) {
          discardResult = true ;
          return ;
        } else {
          CommandPayload payload = registry.getDataAs(path, CommandPayload.class) ;
          result = payload.getResult() ;
          registry.delete(path);
        }
      } catch(RegistryException e) {
        error = e ;
        if(e.getErrorCode() == ErrorCode.NoNode) {
          discardResult = true ;
        }
      } finally {
        notifyForResult() ;
      }
    }
    synchronized void notifyForResult() {
      notifyAll() ;
    }
    
    synchronized public CommandResult<?> waitForResult(long timeout) throws Exception {
      if(result == null) {
        wait(timeout);
      }
      if(discardResult) {
        result = new CommandResult<Object>() ;
        result.setDiscardResult(true);
      }
      if(error != null) throw error;
      if(result == null) {
        String errorMsg = "Cannot get the result after " + timeout + "ms, command = " + command.getClass() +", path = " + path ;
        throw new TimeoutException(errorMsg) ;
      }
      return result ;
    }
  }
}