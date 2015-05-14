package com.neverwinterdp.scribengin.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.DataMapperCallback;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.queue.DistributedQueue;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor.Status;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig;
import com.neverwinterdp.scribengin.dataflow.util.DataflowTaskNodeDebugger;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
public class DataflowRegistry {
  final static public String TASKS_PATH = "tasks";
  
  final static public String TASKS_DESCRIPTORS_PATH = TASKS_PATH + "/descriptors";
  final static public String TASKS_AVAILABLE_PATH   = TASKS_PATH + "/executors/available";
  final static public String TASKS_ASSIGNED_PATH    = TASKS_PATH + "/executors/assigned" ;
  final static public String TASKS_FINISHED_PATH    = TASKS_PATH + "/executors/finished";
  final static public String TASKS_LOCK_PATH        = TASKS_PATH + "/executors/locks";

  final static public String MASTER_EVENT_PATH      = "event/master" ;
  final static public String WORKER_EVENT_PATH      = "event/worker" ;
  final static public String FAILURE_EVENT_PATH     = "event/failure" ;
  
  final static public String ACTIVITIES_PATH           = "activities";
  
  final static public String MASTER_PATH            = "master";
  final static public String MASTER_LEADER_PATH     = MASTER_PATH + "/leader";
  
  final static public String ALL_WORKERS_PATH       = "workers/all";
  final static public String ACTIVE_WORKERS_PATH    = "workers/active";
  final static public String HISTORY_WORKERS_PATH   = "workers/history";
  
  final static public DataMapperCallback<DataflowTaskDescriptor> TASK_DESCRIPTOR_DATA_MAPPER = new DataMapperCallback<DataflowTaskDescriptor>() {
    @Override
    public DataflowTaskDescriptor map(String path, byte[] data, Class<DataflowTaskDescriptor> type) {
      DataflowTaskDescriptor descriptor = JSONSerializer.INSTANCE.fromBytes(data, type);
      descriptor.setRegistryPath(path);
      return descriptor;
    }
  };
  
  @Inject @Named("dataflow.registry.path") 
  private String             dataflowPath;
  @Inject
  private Registry           registry;
  
  private Node               status;
  
  private Node               tasksDescriptors;
  private Node               workerEventNode;
  private Node               failureEventNode;
  private Node               masterEventNode;
  private DistributedQueue   tasksAvailableQueue;
  private Node               tasksAssignedNode;
  private Node               tasksFinishedNode;
  private Node               tasksLock;
  private Node               activeActivitiesNode;
  
  private Node               allWorkers;
  private Node               activeWorkers;
  private Node               historyWorkers;
  


  public DataflowRegistry() { }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws Exception { 
    this.registry = registry;
    this.dataflowPath = dataflowPath;
    onInit();
  }
  
  @Inject
  public void onInit() throws Exception {
    registry.createIfNotExist(dataflowPath + "/" + MASTER_LEADER_PATH);
    status = registry.createIfNotExist(dataflowPath + "/status");
    tasksDescriptors = registry.createIfNotExist(dataflowPath + "/" + TASKS_DESCRIPTORS_PATH);
    
    masterEventNode        = registry.createIfNotExist(dataflowPath + "/" + MASTER_EVENT_PATH);
    workerEventNode  = registry.createIfNotExist(dataflowPath + "/" + WORKER_EVENT_PATH);
    failureEventNode = registry.createIfNotExist(dataflowPath + "/" + FAILURE_EVENT_PATH);
    
    tasksAvailableQueue = new DistributedQueue(registry, dataflowPath + "/" + TASKS_AVAILABLE_PATH);
    tasksAssignedNode = registry.createIfNotExist(dataflowPath + "/" + TASKS_ASSIGNED_PATH);
    tasksFinishedNode = registry.createIfNotExist(dataflowPath + "/" + TASKS_FINISHED_PATH);
    tasksLock = registry.createIfNotExist(dataflowPath + "/" + TASKS_LOCK_PATH);
    
    activeActivitiesNode = registry.createIfNotExist(dataflowPath + "/" + ACTIVITIES_PATH);
    
    allWorkers = registry.createIfNotExist(dataflowPath + "/" + ALL_WORKERS_PATH);
    activeWorkers = registry.createIfNotExist(dataflowPath + "/" + ACTIVE_WORKERS_PATH);
    historyWorkers = registry.createIfNotExist(dataflowPath + "/" + HISTORY_WORKERS_PATH);
  }
  
  public String getDataflowPath() { return this.dataflowPath ; }
  
  public Node getWorkerEventNode() { return workerEventNode ; }

  public Node getMasterEventNode() { return masterEventNode ; }
  
  public Node getFailureEventNode() { return failureEventNode ; }
  
  public Node getTasksFinishedNode() { return tasksFinishedNode;}
  
  public Node getTasksAssignedNode() { return tasksAssignedNode; }
  
  public Node getActiveActivitiesNode() { return activeActivitiesNode; } 
  
  public Registry getRegistry() { return this.registry ; }
  
  public DataflowDescriptor getDataflowDescriptor() throws RegistryException {
    return registry.getDataAs(dataflowPath, DataflowDescriptor.class);
  }
  
  //TODO: find where this method is used and pass an array of the descriptor and execute the add in the a transaction
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    Node taskNode = tasksDescriptors.createChild("task-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    taskDescriptor.setRegistryPath(taskNode.getPath());
    taskNode.setData(taskDescriptor);
    DataflowTaskReport report = new DataflowTaskReport();
    report.setStartTime(System.currentTimeMillis());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
    String nodeName = taskNode.getName();
    tasksAvailableQueue.offer(nodeName.getBytes());
   }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    RefNode refNode = new RefNode(vmDescriptor.getRegistryPath()) ;
    transaction.createChild(allWorkers, vmDescriptor.getId(), refNode, NodeCreateMode.PERSISTENT) ;
    transaction.createDescendant(allWorkers, vmDescriptor.getId() + "/status", DataflowWorkerStatus.INIT, NodeCreateMode.PERSISTENT) ;
    transaction.createChild(activeWorkers, vmDescriptor.getId(), NodeCreateMode.PERSISTENT) ;
    transaction.commit();
  }
  
  public void setWorkerStatus(VMDescriptor vmDescriptor, DataflowWorkerStatus status) throws RegistryException {
    Node workerNode = allWorkers.getChild(vmDescriptor.getId());
    Node statusNode = workerNode.getChild("status");
    statusNode.setData(status);
  }
  
  public void historyWorker(String vmId) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    transaction.createChild(historyWorkers, vmId, NodeCreateMode.PERSISTENT) ;
    transaction.deleteChild(activeWorkers, vmId) ;
    transaction.commit();
  }
  
  public void createWorkerTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = allWorkers.getChild(vmDescriptor.getId()) ;
    Node executors = worker.createDescendantIfNotExists("executors");
    executors.createChild(descriptor.getId(), descriptor, NodeCreateMode.PERSISTENT);
  }
  
  public void updateWorkerTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = allWorkers.getChild(vmDescriptor.getId()) ;
    Node executor = worker.getDescendant("executors/" + descriptor.getId()) ;
    executor.setData(descriptor);
  }
  
  public Node getStatusNode() { return this.status ; }
  
  public DataflowLifecycleStatus getStatus() throws RegistryException {
    return status.getDataAs(DataflowLifecycleStatus.class) ;
  }
  
  public void setStatus(DataflowLifecycleStatus event) throws RegistryException {
    status.setData(event);
  }
  
  public <T> void broadcastWorkerEvent(T event) throws RegistryException {
    workerEventNode.setData(event);
  }
  
  public void broadcastMasterEvent(DataflowEvent event) throws RegistryException {
    masterEventNode.setData(event);
  }
  
  public void broadcastFailureEvent(FailureConfig event) throws RegistryException {
    failureEventNode.setData(event);
  }
  
  public DataflowTaskDescriptor assignDataflowTask(final VMDescriptor vmDescriptor) throws RegistryException  {
    Lock lock = tasksLock.getLock("write", "Lock to assign task to " + vmDescriptor.getId()) ;
    BatchOperations<DataflowTaskDescriptor> getAssignedtaskOp = new BatchOperations<DataflowTaskDescriptor>() {
      @Override
      public DataflowTaskDescriptor execute(Registry registry) throws RegistryException {
        byte[] data  = tasksAvailableQueue.poll();
        if(data == null) return null;
        String taskName = new String(data);
        Node childNode = tasksDescriptors.getChild(taskName);
        Transaction transaction = registry.getTransaction();
        transaction.createChild(tasksAssignedNode, taskName, NodeCreateMode.PERSISTENT);
        transaction.createDescendant(tasksAssignedNode, taskName + "/heartbeat", vmDescriptor, NodeCreateMode.EPHEMERAL);
        transaction.commit();
        
        DataflowTaskDescriptor descriptor = childNode.getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
        return descriptor;
      }
    };
    return lock.execute(getAssignedtaskOp, 5, 1000);
  }
  
  
  public void dataflowTaskSuspend(final DataflowTaskDescriptor descriptor) throws RegistryException {
    Lock lock = tasksLock.getLock("write", "Lock to move the task " + descriptor.getId() + " to suspend") ;
    BatchOperations<Boolean> suspendtOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        //TODO: use the transaction
        
        descriptor.setStatus(Status.SUSPENDED);    
        dataflowTaskUpdate(descriptor);
        Node descriptorNode = registry.get(descriptor.getRegistryPath()) ;
        String name = descriptorNode.getName();
        tasksAvailableQueue.offer(name.getBytes());
        //tasksAssignedNode.getChild(dataflowName).rdelete();
        Transaction transaction = registry.getTransaction();
        transaction.rdelete(tasksAssignedNode.getPath() + "/" + name);
        transaction.commit();
        return true;
      }
    };
    lock.execute(suspendtOp, 5, 1000);
  }

  public void dataflowTaskFinish(final DataflowTaskDescriptor descriptor) throws RegistryException {
    Lock lock = tasksLock.getLock("write", "Lock to move the task " + descriptor.getId() + " to finish") ;
    BatchOperations<Boolean> commitOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node descriptorNode = registry.get(descriptor.getRegistryPath()) ;
        String name = descriptorNode.getName();
        descriptor.setStatus(Status.TERMINATED);
        //TODO Transaction: convert to use the transaction
        Transaction transaction = registry.getTransaction();
        //update the task descriptor
        transaction.setData(descriptor.getRegistryPath(), descriptor);
        transaction.createChild(tasksFinishedNode, name, NodeCreateMode.PERSISTENT);
        transaction.rdelete(tasksAssignedNode.getPath() + "/" + name);
        //tasksFinishedNode.createChild(transaction, dataflowName, NodeCreateMode.PERSISTENT);
        //tasksAssignedNode.getChild(dataflowName).rdelete(transaction);
        transaction.commit();
        return true;
      }
    };
    lock.execute(commitOp, 5, 1000);
  }
  
  public void dataflowTaskUpdate(DataflowTaskDescriptor descriptor) throws RegistryException {
    registry.setData(descriptor.getRegistryPath(), descriptor);
  }
  
  public void dataflowTaskReport(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node reportNode = registry.get(descriptor.getRegistryPath() + "/report");
    reportNode.setData(report);
  }
  
  public void create(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node taskNode = registry.get(descriptor.getRegistryPath());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public List<DataflowTaskDescriptor> getTaskDescriptors() throws RegistryException {
    return tasksDescriptors.getChildrenAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskDescriptor getTaskDescriptor(String taskName) throws RegistryException {
    return tasksDescriptors.getChild(taskName).getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskReport getTaskReport(DataflowTaskDescriptor descriptor) throws RegistryException {
    return registry.getDataAs(descriptor.getRegistryPath() + "/report", DataflowTaskReport.class) ;
  }
  
  public List<DataflowTaskReport> getTaskReports(List<DataflowTaskDescriptor> descriptors) throws RegistryException {
    List<String> reportPaths = new ArrayList<String>();
    for(int i = 0; i < descriptors.size(); i++) {
      reportPaths.add(descriptors.get(i).getRegistryPath() + "/report") ;
    }
    return registry.getDataAs(reportPaths, DataflowTaskReport.class) ;
  }
  
  public VMDescriptor getDataflowMaster() throws RegistryException {
    String leaderPath = dataflowPath + "/" + MASTER_LEADER_PATH;
    Node node = registry.getRef(leaderPath);
    return node.getDataAs(VMDescriptor.class);
  }
  
  public List<VMDescriptor> getDataflowMasters() throws RegistryException {
    return registry.getRefChildrenAs(dataflowPath + "/" + MASTER_PATH, VMDescriptor.class);
  }
  
  public int countDataflowMasters() throws RegistryException {
    return registry.getChildren(dataflowPath + "/" + MASTER_LEADER_PATH ).size();
  }
  
  
  public List<VMDescriptor> getActiveWorkers() throws RegistryException {
    List<String> activeWorkerIds = activeWorkers.getChildren();
    return allWorkers.getSelectRefChildrenAs(activeWorkerIds, VMDescriptor.class) ;
  }
  
  public int countActiveDataflowWorkers() throws RegistryException {
    return registry.getChildren(dataflowPath + "/" + ACTIVE_WORKERS_PATH).size();
  }
  
  public Node getAllWorkersNode() { return allWorkers ; }
  
  public Node getActiveWorkersNode() { return activeWorkers ; }
  
  public Node getWorkerNode(String vmId) throws RegistryException { 
    return allWorkers.getChild(vmId) ; 
  }
  
  public List<String> getAllWorkerNames() throws RegistryException {
    return allWorkers.getChildren();
  }
  
  public List<String> getActiveWorkerNames() throws RegistryException {
    return activeWorkers.getChildren();
  }
  
  public DataflowWorkerStatus getDataflowWorkerStatus(String vmId) throws RegistryException {
    return allWorkers.getChild(vmId).getChild("status").getDataAs(DataflowWorkerStatus.class);
  }
  
  public List<DataflowTaskExecutorDescriptor> getWorkerExecutors(String worker) throws RegistryException {
    Node executors = allWorkers.getDescendant(worker + "/executors") ;
    return executors.getChildrenAs(DataflowTaskExecutorDescriptor.class);
  }
  
  public RegistryDebugger getDataflowTaskDebugger(Appendable out) throws RegistryException {
    RegistryDebugger debugger = new RegistryDebugger(out, registry) ;
    System.out.println("dataflow task debug path:");
    System.out.println("  " + tasksAssignedNode.getPath());
    System.out.println("  /scribengin/dataflows/running/hello-kafka-dataflow/tasks/executors/assigned");
    debugger.watchChild(tasksAssignedNode.getPath(), ".*", new DataflowTaskNodeDebugger());
    return debugger ;
  }
  
  public void dump() throws RegistryException, IOException {
    registry.get(dataflowPath).dump(System.out);
  }

  public ActivityRegistry getActivityRegistry() throws RegistryException {
    return new ActivityRegistry(registry, dataflowPath + "/" + ACTIVITIES_PATH) ;
  }
  
  static  public DataflowLifecycleStatus getStatus(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getDataAs(dataflowPath + "/status" , DataflowLifecycleStatus.class) ;
  }
}
