package com.neverwinterdp.scribengin.dataflow;

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
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.queue.DistributedQueue;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor.Status;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
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
  
  final static public String ACTIVITIES_PATH        = "activities";
  final static public String MASTER_PATH            = "master";
  final static public String MASTER_LEADER_PATH     = MASTER_PATH + "/leader";
  
  final static public String ACTIVE_WORKERS_PATH    = "workers/active";
  final static public String HISTORY_WORKERS_PATH   = "workers/history";

  final static public DataMapperCallback<DataflowTaskDescriptor> TASK_DESCRIPTOR_DATA_MAPPER = new DataMapperCallback<DataflowTaskDescriptor>() {
    @Override
    public DataflowTaskDescriptor map(String path, byte[] data, Class<DataflowTaskDescriptor> type) {
      DataflowTaskDescriptor descriptor = JSONSerializer.INSTANCE.fromBytes(data, type);
      descriptor.setStoredPath(path);
      return descriptor;
    }
  };
  
  @Inject @Named("dataflow.registry.path") 
  private String             dataflowPath;
  @Inject
  private Registry           registry;
  private Node               status;
  
  private Node               tasksDescriptors;
  private Node               dataflowWorkerEvent;
  private Node               dataflowEvent;
  private DistributedQueue   tasksAvailableQueue;
  private Node               tasksAssigned;
  private Node               tasksFinished;
  private Node               tasksLock;
  private Node               activities;
  
  private Node               activeWorkers;
  private Node               historyWorkers;


  public DataflowRegistry() { }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws Exception { 
    this.registry = registry;
    this.dataflowPath = dataflowPath;
    onInit();
  }
  
  public String getDataflowPath() { return this.dataflowPath ; }
  
  public Node getDataflowTasksWorkerEventNode() { return dataflowWorkerEvent ; }

  public Node getDataflowTasksMasterEventNode() { return dataflowEvent ; }
  
  public Node getTasksFinishedNode() { return tasksFinished;}
  
  public String getTasksAssignedPath() { return this.tasksAssigned.getPath(); }
  
  public String getActivitiesPath() { return activities.getPath(); } 
  
  public Registry getRegistry() { return this.registry ; }
  
  public DataflowDescriptor getDataflowDescriptor() throws RegistryException {
    return registry.getDataAs(dataflowPath, DataflowDescriptor.class);
  }
  
  @Inject
  public void onInit() throws Exception {
    registry.createIfNotExist(dataflowPath + "/" + MASTER_LEADER_PATH);
    status = registry.createIfNotExist(dataflowPath + "/status");
    tasksDescriptors = registry.createIfNotExist(dataflowPath + "/" + TASKS_DESCRIPTORS_PATH);
    dataflowWorkerEvent = registry.createIfNotExist(dataflowPath + "/" + WORKER_EVENT_PATH);
    dataflowEvent = registry.createIfNotExist(dataflowPath + "/" + MASTER_EVENT_PATH);
    tasksAvailableQueue = new DistributedQueue(registry, dataflowPath + "/" + TASKS_AVAILABLE_PATH);
    tasksAssigned = registry.createIfNotExist(dataflowPath + "/" + TASKS_ASSIGNED_PATH);
    tasksFinished = registry.createIfNotExist(dataflowPath + "/" + TASKS_FINISHED_PATH);
    tasksLock = registry.createIfNotExist(dataflowPath + "/" + TASKS_LOCK_PATH);
    activities = registry.createIfNotExist(dataflowPath + "/" + ACTIVITIES_PATH);
    activeWorkers = registry.createIfNotExist(dataflowPath + "/" + ACTIVE_WORKERS_PATH);
    historyWorkers = registry.createIfNotExist(dataflowPath + "/" + HISTORY_WORKERS_PATH);
  }
  
  //TODO: find where this method is used and pass an array of the descriptor and execute the add in the a transaction
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    Node taskNode = tasksDescriptors.createChild("task-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    taskDescriptor.setStoredPath(taskNode.getPath());
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
    transaction.createChild(activeWorkers, vmDescriptor.getId(), refNode, NodeCreateMode.PERSISTENT) ;
    transaction.createDescendant(activeWorkers, vmDescriptor.getId() + "/status", DataflowWorkerStatus.INIT, NodeCreateMode.PERSISTENT) ;
    transaction.commit();
  }
  
  public void setWorkerStatus(VMDescriptor vmDescriptor, DataflowWorkerStatus status) throws RegistryException {
    Node workerNode = activeWorkers.getChild(vmDescriptor.getId());
    Node statusNode = workerNode.getChild("status");
    statusNode.setData(status);
  }
  
  public void historyWorker(VMDescriptor vmDescriptor) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    String fromPath = activeWorkers.getChild(vmDescriptor.getId()).getPath() ;
    String toPath   = historyWorkers.getChild(vmDescriptor.getId()).getPath();
    transaction.rcopy(fromPath, toPath);
    transaction.rdelete(fromPath);
    transaction.commit();
  }
  
  public void createWorkerTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = activeWorkers.getChild(vmDescriptor.getId()) ;
    Node executors = worker.createDescendantIfNotExists("executors");
    executors.createChild(descriptor.getId(), descriptor, NodeCreateMode.PERSISTENT);
  }
  
  public void updateWorkerTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = activeWorkers.getChild(vmDescriptor.getId()) ;
    Node executor = worker.getDescendant("executors/" + descriptor.getId()) ;
    executor.setData(descriptor);
  }
  
  public DataflowLifecycleStatus getStatus() throws RegistryException {
    return status.getDataAs(DataflowLifecycleStatus.class) ;
  }
  
  public void setStatus(DataflowLifecycleStatus event) throws RegistryException {
    status.setData(event);
  }
  
  public <T> void broadcastDataflowWorkerEvent(T event) throws RegistryException {
    dataflowWorkerEvent.setData(event);
  }
  
  public void broadcastDataflowEvent(DataflowEvent event) throws RegistryException {
    dataflowEvent.setData(event);
  }
  
  
  public DataflowTaskDescriptor assignDataflowTask(final VMDescriptor vmDescriptor) throws RegistryException  {
    Lock lock = tasksLock.getLock("write") ;
    BatchOperations<DataflowTaskDescriptor> getAssignedtaskOp = new BatchOperations<DataflowTaskDescriptor>() {
      @Override
      public DataflowTaskDescriptor execute(Registry registry) throws RegistryException {
        byte[] data  = tasksAvailableQueue.poll();
        if(data == null) return null;
        String taskName = new String(data);
        Node childNode = tasksDescriptors.getChild(taskName);
        Transaction transaction = registry.getTransaction();
        transaction.createChild(tasksAssigned, taskName, NodeCreateMode.PERSISTENT);
        transaction.createDescendant(tasksAssigned, taskName + "/heartbeat", vmDescriptor, NodeCreateMode.EPHEMERAL);
        transaction.commit();
        
        DataflowTaskDescriptor descriptor = childNode.getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
        return descriptor;
      }
    };
    return lock.execute(getAssignedtaskOp, 5, 1000);
  }
  
  
  public void dataflowTaskSuspend(final DataflowTaskDescriptor descriptor) throws RegistryException {
    Lock lock = tasksLock.getLock("write") ;
    BatchOperations<Boolean> suspendtOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        //TODO: use the transaction
        
        descriptor.setStatus(Status.SUSPENDED);    
        dataflowTaskUpdate(descriptor);
        Node descriptorNode = registry.get(descriptor.getStoredPath()) ;
        String name = descriptorNode.getName();
        tasksAvailableQueue.offer(name.getBytes());
        //tasksAssigned.getChild(dataflowName).rdelete();
        Transaction transaction = registry.getTransaction();
        transaction.rdelete(tasksAssigned.getPath() + "/" + name);
        transaction.commit();
        return true;
      }
    };
    lock.execute(suspendtOp, 5, 1000);
  }

  public void dataflowTaskFinish(final DataflowTaskDescriptor descriptor) throws RegistryException {
    Lock lock = tasksLock.getLock("write") ;
    BatchOperations<Boolean> commitOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node descriptorNode = registry.get(descriptor.getStoredPath()) ;
        String name = descriptorNode.getName();
        descriptor.setStatus(Status.TERMINATED);
        //TODO Transaction: convert to use the transaction
        Transaction transaction = registry.getTransaction();
        //update the task descriptor
        transaction.setData(descriptor.getStoredPath(), descriptor);
        transaction.createChild(tasksFinished, name, NodeCreateMode.PERSISTENT);
        transaction.rdelete(tasksAssigned.getPath() + "/" + name);
        //tasksFinished.createChild(transaction, dataflowName, NodeCreateMode.PERSISTENT);
        //tasksAssigned.getChild(dataflowName).rdelete(transaction);
        transaction.commit();
        return true;
      }
    };
    lock.execute(commitOp, 5, 1000);
  }
  
  public void dataflowTaskUpdate(DataflowTaskDescriptor descriptor) throws RegistryException {
    registry.setData(descriptor.getStoredPath(), descriptor);
  }
  
  public void dataflowTaskReport(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node reportNode = registry.get(descriptor.getStoredPath() + "/report");
    reportNode.setData(report);
  }
  
  public void create(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node taskNode = registry.get(descriptor.getStoredPath());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public List<DataflowTaskDescriptor> getTaskDescriptors() throws RegistryException {
    return tasksDescriptors.getChildrenAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskDescriptor getTaskDescriptor(String taskName) throws RegistryException {
    return tasksDescriptors.getChild(taskName).getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskReport getTaskReport(DataflowTaskDescriptor descriptor) throws RegistryException {
    return registry.getDataAs(descriptor.getStoredPath() + "/report", DataflowTaskReport.class) ;
  }
  
  public List<DataflowTaskReport> getTaskReports(List<DataflowTaskDescriptor> descriptors) throws RegistryException {
    List<String> reportPaths = new ArrayList<String>();
    for(int i = 0; i < descriptors.size(); i++) {
      reportPaths.add(descriptors.get(i).getStoredPath() + "/report") ;
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
  
  public List<VMDescriptor> getActiveWorkers() throws RegistryException {
    return registry.getRefChildrenAs(dataflowPath + "/" + ACTIVE_WORKERS_PATH, VMDescriptor.class);
  }
  
  public Node getActiveWorkersNode() { return activeWorkers ; }
  
  public List<String> getActiveWorkerNames() throws RegistryException {
    return activeWorkers.getChildren();
  }
  
  public List<DataflowTaskExecutorDescriptor> getActiveExecutors(String worker) throws RegistryException {
    Node executors = activeWorkers.getDescendant(worker + "/executors") ;
    return executors.getChildrenAs(DataflowTaskExecutorDescriptor.class);
  }
  
  public RegistryDebugger getDataflowTaskDebugger(Appendable out) throws RegistryException {
    RegistryDebugger debugger = new RegistryDebugger(out, registry) ;
    System.out.println("dataflow task debug path:");
    System.out.println("  " + tasksAssigned.getPath());
    System.out.println("  /scribengin/dataflows/running/hello-kafka-dataflow/tasks/executors/assigned");
    debugger.watchChild(tasksAssigned.getPath(), ".*", new DataflowTaskNodeDebugger());
    return debugger ;
  }
}
