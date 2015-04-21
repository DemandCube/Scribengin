package com.neverwinterdp.scribengin;

import static com.neverwinterdp.vm.tool.VMClusterBuilder.h1;

import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.activity.util.ActiveActivityNodeDebugger;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.dataflow.util.DataflowTaskNodeDebugger;
import com.neverwinterdp.scribengin.dataflow.util.DataflowVMDebugger;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.scribengin.service.VMScribenginServiceApp;
import com.neverwinterdp.scribengin.service.VMScribenginServiceCommand;
import com.neverwinterdp.util.JSONSerializer;
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
  
  public VMClient getVMClient() { 
 //   System.err.println("vmclient "+ vmClient);
    return this.vmClient ; }
  
  public VMDescriptor getScribenginMaster() throws RegistryException {
    Node node = vmClient.getRegistry().getRef(ScribenginService.LEADER_PATH);
    VMDescriptor descriptor = node.getDataAs(VMDescriptor.class);
    return descriptor;
  }
  
  public List<VMDescriptor> getScribenginMasters() throws RegistryException {
    Registry registry = vmClient.getRegistry();
    List<VMDescriptor> vmDescriptors = registry.getRefChildrenAs(ScribenginService.LEADER_PATH, VMDescriptor.class) ;
    return vmDescriptors;
  }
  
  public List<DataflowDescriptor> getRunningDataflowDescriptor() throws RegistryException {
    List<DataflowDescriptor> holder =
      vmClient.getRegistry().getChildrenAs(ScribenginService.DATAFLOWS_RUNNING_PATH, DataflowDescriptor.class) ;
    holder.removeAll(Collections.singleton(null));
    return holder;
  }
  
  public List<DataflowDescriptor> getHistoryDataflowDescriptor() throws RegistryException {
    List<DataflowDescriptor> holder = 
        vmClient.getRegistry().getChildrenAs(ScribenginService.DATAFLOWS_HISTORY_PATH, DataflowDescriptor.class) ;
    holder.removeAll(Collections.singleton(null));
    return holder;
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
  
  public DataflowWaitingEventListener submit(String dataflowAppHome, String jsonDescriptor) throws Exception {
    DataflowDescriptor descriptor = JSONSerializer.INSTANCE.fromString(jsonDescriptor, DataflowDescriptor.class) ;
    return submit(dataflowAppHome, descriptor) ;
  }
  
  public DataflowWaitingEventListener submit(DataflowDescriptor descriptor) throws Exception {
    return submit(null, descriptor) ;
  }
  
  public DataflowWaitingEventListener submit(String localDataflowHome, DataflowDescriptor descriptor) throws Exception {
    if(localDataflowHome != null) {
      VMDescriptor vmMaster = getVMClient().getMasterVMDescriptor();
      VMConfig vmConfig = vmMaster.getVmConfig();
      String dataflowAppHome = vmConfig.getAppHome() + "/dataflows/" + descriptor.getName();
      descriptor.setDataflowAppHome(dataflowAppHome);
      getVMClient().uploadApp(localDataflowHome, dataflowAppHome);
    }
    h1("Submit the dataflow " + descriptor.getName());
    String name = descriptor.getName() ;
    VMClient vmClient = new VMClient(getRegistry());
    
    DataflowWaitingEventListener waitingEventListener = new DataflowWaitingEventListener(vmClient.getRegistry());
    waitingEventListener.waitDataflowLeader(format("Expect %s-master-1 as the leader", name), name,  format("%s-master-1", name));
    waitingEventListener.waitDataflowStatus("Expect dataflow init status", name, DataflowLifecycleStatus.INIT);
    waitingEventListener.waitDataflowStatus("Expect dataflow running status", name, DataflowLifecycleStatus.RUNNING);
    waitingEventListener.waitDataflowStatus("Expect dataflow  finish status", name, DataflowLifecycleStatus.FINISH);
   
    VMDescriptor scribenginMaster = getScribenginMaster();
    Command deployCmd = new VMScribenginServiceCommand.DataflowDeployCommand(descriptor) ;
    CommandResult<Boolean> result = (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    return waitingEventListener;
  }
  
  private String format(String tmpl, Object ... args) {
    return String.format(tmpl, args) ;
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
  
  public DataflowClient getDataflowClient(String dataflowName) throws Exception {
    return getDataflowClient(dataflowName, 60000) ;
  }
  
  public DataflowClient getDataflowClient(String dataflowName, long timeout) throws Exception {
    String dataflowPath = ScribenginService.getDataflowPath(dataflowName);
    long stopTime = System.currentTimeMillis() + timeout;
    while(System.currentTimeMillis() < stopTime) {
      if(getRegistry().exists(dataflowPath + "/status")) {
        DataflowLifecycleStatus status = 
            getRegistry().get(dataflowPath + "/status").getDataAs(DataflowLifecycleStatus.class) ;
        if(status == DataflowLifecycleStatus.RUNNING ) {
          DataflowClient dataflowClient = new DataflowClient(this, dataflowPath);
          return dataflowClient ;
        }
      }
      Thread.sleep(500);
    }
    throw new Exception("The dataflow " + dataflowName + " is not existed after " + timeout + "ms");
  }
  
  public RegistryDebugger getDataflowTaskDebugger(Appendable out, String dataflowName, boolean detailedDebugger) throws RegistryException {
    String taskAssignedPath = ScribenginService.getDataflowPath(dataflowName) + "/" + DataflowRegistry.TASKS_ASSIGNED_PATH;
    RegistryDebugger debugger = new RegistryDebugger(out, getVMClient().getRegistry()) ;
    debugger.watchChild(taskAssignedPath, ".*", new DataflowTaskNodeDebugger(detailedDebugger));
    return debugger ;
  }
  
  public RegistryDebugger getDataflowActivityDebugger(Appendable out, String dataflowName, boolean detailedDebugger) throws RegistryException {
    String activeActivitiesPath = ScribenginService.getDataflowPath(dataflowName) + "/activities/active" ;
    RegistryDebugger debugger = new RegistryDebugger(out, getVMClient().getRegistry()) ;
    debugger.watchChild(activeActivitiesPath, ".*", new ActiveActivityNodeDebugger(detailedDebugger));
    return debugger ;
  }
  
  public RegistryDebugger getDataflowVMDebugger(Appendable out, String dataflowName, boolean detailedDebugger) throws RegistryException {
    String workerActivePath = ScribenginService.getDataflowPath(dataflowName) + "/workers/active";
    RegistryDebugger debugger = new RegistryDebugger(out, getVMClient().getRegistry()) ;
    debugger.watchChild(workerActivePath,  ".*", new DataflowVMDebugger(detailedDebugger));
    return debugger ;
  }
  
  
  
  public void shutdown() throws Exception {
    h1("Shutdow the scribengin");
    List<VMDescriptor> masters = getScribenginMasters() ;
    VMDescriptor activeMaster = getScribenginMaster() ;
    for(int i = 0; i < masters.size(); i++) {
      VMDescriptor master = masters.get(i) ;
      if(!master.getId().equals(activeMaster.getId())) {
        vmClient.shutdown(master) ;
      }
    }
    Thread.sleep(1000);
    vmClient.shutdown(activeMaster) ;
    //Registry registry = vmClient.getRegistry();
    //registry.create(ScribenginService.SHUTDOWN_EVENT_PATH, true, NodeCreateMode.PERSISTENT);
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command) throws RegistryException, Exception {
    return execute(vmDescriptor, command, 30000);
  }
  
  public CommandResult<?> execute(VMDescriptor vmDescriptor, Command command, long timeout) throws RegistryException, Exception {
    CommandPayload payload = new CommandPayload(command, null) ;
    Registry registry = vmClient.getRegistry();
    Node node = registry.create(vmDescriptor.getRegistryPath() + "/commands/command-", payload, NodeCreateMode.EPHEMERAL_SEQUENTIAL);
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
