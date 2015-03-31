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
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.queue.DistributedQueue;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor.Status;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMDescriptor;

//TODO: Nizar
@Singleton
public class DataflowRegistry {
  final static public String TASKS_PATH = "tasks";
  final static public String TASKS_DESCRIPTORS_PATH = TASKS_PATH + "/descriptors";
  final static public String TASKS_AVAILABLE_PATH   = TASKS_PATH + "/executors/available";
  final static public String TASKS_ASSIGNED_PATH    = TASKS_PATH + "/executors/assigned" ;
  final static public String TASKS_FINISHED_PATH    = TASKS_PATH + "/executors/finished";
  final static public String TASKS_LOCK_PATH        = TASKS_PATH + "/executors/locks";

  final static public String ACTIVITIES_PATH = "activities";
  final static public String MASTER_PATH  = "master";
  final static public String MASTER_LEADER_PATH  = MASTER_PATH + "/leader";
  
  final static public String WORKERS_PATH = "workers";

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
  //private Node               tasksAvailable;
  private DistributedQueue   tasksAvailableQueue;
  private Node               tasksAssigned;
  private Node               tasksFinished;
  private Node               tasksLock;
  private Node               activities;
  private Node               workers;


  public DataflowRegistry() { }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws Exception { 
    this.registry = registry;
    this.dataflowPath = dataflowPath;
    onInit();
  }
  
  public String getDataflowPath() { return this.dataflowPath ; }
  
  public String getTasksFinishedPath() { return tasksFinished.getPath() ;}
  
  public String getTasksAssignedPath() { return this.tasksAssigned.getPath(); }
  
  public String getActivitiesPath() { return activities.getPath(); } 
  
  public Registry getRegistry() { return this.registry ; }
  
  public DataflowDescriptor getDataflowDescriptor() throws RegistryException {
    return registry.getDataAs(dataflowPath, DataflowDescriptor.class);
  }
  
  @Inject
  public void onInit() throws Exception {
    status         = registry.createIfNotExist(dataflowPath + "/status");
    tasksDescriptors = registry.createIfNotExist(dataflowPath + "/" + TASKS_DESCRIPTORS_PATH);
    tasksAvailableQueue = new DistributedQueue(registry, dataflowPath   + "/" + TASKS_AVAILABLE_PATH) ;
    tasksAssigned  = registry.createIfNotExist(dataflowPath   + "/" + TASKS_ASSIGNED_PATH);
    tasksFinished  = registry.createIfNotExist(dataflowPath   + "/" + TASKS_FINISHED_PATH);
    tasksLock      = registry.createIfNotExist(dataflowPath   + "/" + TASKS_LOCK_PATH);
    registry.createIfNotExist(dataflowPath + "/" + MASTER_LEADER_PATH);
    activities = registry.createIfNotExist(dataflowPath + "/" + ACTIVITIES_PATH);
    workers = registry.createIfNotExist(dataflowPath + "/" + WORKERS_PATH);
    
  }
  
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
    workers.createChildRef(vmDescriptor.getId(), vmDescriptor.getStoredPath(), NodeCreateMode.PERSISTENT);
  }
  
  public void setStatus(DataflowLifecycleStatus event) throws RegistryException {
    status.setData(event);
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
  
  
  public void createTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = workers.getChild(vmDescriptor.getId()) ;
    Node executors = worker.createDescendantIfNotExists("executors");
    executors.createChild(descriptor.getId(), descriptor, NodeCreateMode.PERSISTENT);
  }
  
  public void updateTaskExecutor(VMDescriptor vmDescriptor, DataflowTaskExecutorDescriptor descriptor) throws RegistryException {
    Node worker = workers.getChild(vmDescriptor.getId()) ;
    Node executor = worker.getDescendant("executors/" + descriptor.getId()) ;
    executor.setData(descriptor);
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
        //tasksAssigned.getChild(name).rdelete();
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
        //tasksFinished.createChild(transaction, name, NodeCreateMode.PERSISTENT);
        //tasksAssigned.getChild(name).rdelete(transaction);
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
  
  public List<VMDescriptor> getWorkers() throws RegistryException {
    return registry.getRefChildrenAs(dataflowPath + "/" + WORKERS_PATH, VMDescriptor.class);
  }
  
  public List<String> getWorkerNames() throws RegistryException {
    return workers.getChildren();
  }
  
  public List<DataflowTaskExecutorDescriptor> getExecutors(String worker) throws RegistryException {
    Node executors = workers.getDescendant(worker + "/executors") ;
    return executors.getChildrenAs(DataflowTaskExecutorDescriptor.class);
  }
}
