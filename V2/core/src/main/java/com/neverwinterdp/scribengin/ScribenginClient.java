package com.neverwinterdp.scribengin;

import static com.neverwinterdp.vm.builder.VMClusterBuilder.h1;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.event.ScribenginShutdownEventListener;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.scribengin.service.VMScribenginServiceApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandPayload;
import com.neverwinterdp.vm.command.CommandResult;

public class ScribenginClient {
  private VMClient vmClient;

  public ScribenginClient(Registry registry) {
    vmClient = new VMClient(registry);
  }
  
  public ScribenginClient(VMClient vmClient) {
    this.vmClient = vmClient;
  }

  public Registry getRegistry() { return this.vmClient.getRegistry(); }
  
  public VMClient getVMClient() { return this.vmClient ; }
  
  public VMDescriptor getScribenginMaster() throws RegistryException {
    Node node = vmClient.getRegistry().getRef(ScribenginService.LEADER_PATH);
    VMDescriptor descriptor = node.getDataAs(VMDescriptor.class);
    return descriptor;
  }
  
  public List<DataflowDescriptor> getRunningDataflowDescriptor() throws RegistryException {
    return vmClient.getRegistry().getChildrenAs(ScribenginService.DATAFLOWS_RUNNING_PATH, DataflowDescriptor.class) ;
  }
  
  public List<DataflowDescriptor> getHistoryDataflowDescriptor() throws RegistryException {
    return vmClient.getRegistry().getChildrenAs(ScribenginService.DATAFLOWS_HISTORY_PATH, DataflowDescriptor.class) ;
  }
  
  public DataflowRegistry getRunningDataflowRegistry(String name) throws Exception {
    String dataflowPath = ScribenginService.DATAFLOWS_RUNNING_PATH + "/" + name;
    DataflowRegistry dataflowRegistry = new DataflowRegistry(getRegistry(), dataflowPath);
    return dataflowRegistry;
  }
  
  public DataflowRegistry getHistoryDataflowRegistry(String id) throws Exception {
    String dataflowPath = ScribenginService.DATAFLOWS_HISTORY_PATH + "/" + id;
    DataflowRegistry dataflowRegistry = new DataflowRegistry(getRegistry(), dataflowPath);
    return dataflowRegistry;
  }
  
  public VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.
      setName(name).
      addRoles("scribengin-master").
      setRegistryConfig(vmClient.getRegistry().getRegistryConfig()).
      setVmApplication(VMScribenginServiceApp.class.getName());
    vmClient.configureEnvironment(vmConfig);
    VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
    return vmDescriptor;
  }
  
  public void shutdown() throws Exception {
    h1("Shutdow the scribengin");
    Registry registry = vmClient.getRegistry();
    registry.create(ScribenginShutdownEventListener.EVENT_PATH, true, NodeCreateMode.PERSISTENT);
  }
  
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command) throws RegistryException, Exception {
    return execute(vmDescriptor, command, 30000);
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command, long timeout) throws RegistryException, Exception {
    CommandPayload payload = new CommandPayload(command, null) ;
    Registry registry = vmClient.getRegistry();
    Node node = registry.create(vmDescriptor.getStoredPath() + "/commands/command-", payload, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
    CommandReponseWatcher responseWatcher = new CommandReponseWatcher();
    node.watch(responseWatcher);
    return responseWatcher.waitForResult(timeout);
  }
  
  public class CommandReponseWatcher extends NodeWatcher {
    private CommandResult<?> result ;
    private Exception error ;
    
    @Override
    public void onEvent(NodeEvent event) {
      String path = event.getPath();
      try {
        Registry registry = vmClient.getRegistry();
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
