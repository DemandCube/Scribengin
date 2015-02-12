package com.neverwinterdp.scribengin.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.DataMapperCallback;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.lock.LockOperation;
import com.neverwinterdp.registry.queue.DistributedQueue;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor.Status;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
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
  private Node               workers;

  public DataflowRegistry() { }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws Exception { 
    this.registry = registry;
    this.dataflowPath = dataflowPath;
    onInit();
  }
  
  public String getDataflowPath() { return this.dataflowPath ; }
  
  public String getTasksFinishedPath() { return tasksFinished.getPath() ;}
  
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
    workers = registry.createIfNotExist(dataflowPath + "/" + WORKERS_PATH);
  }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    Node taskNode = tasksDescriptors.createChild("task-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    taskDescriptor.setStoredPath(taskNode.getPath());
    taskNode.setData(taskDescriptor);
    taskNode.createChild("report", new DataflowTaskReport(), NodeCreateMode.PERSISTENT);
    String nodeName = taskNode.getName();
    tasksAvailableQueue.offer(nodeName.getBytes());
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    workers.createChildRef(vmDescriptor.getId(), vmDescriptor.getStoredPath(), NodeCreateMode.PERSISTENT);
  }
  
  public void setStatus(DataflowLifecycleStatus event) throws RegistryException {
    status.setData(event);
  }
  
  public DataflowTaskDescriptor getAssignedDataflowTaskDescriptor() throws RegistryException  {
    Lock lock = tasksLock.getLock("write") ;
    LockOperation<DataflowTaskDescriptor> getAssignedtaskOp = new LockOperation<DataflowTaskDescriptor>() {
      @Override
      public DataflowTaskDescriptor execute() throws Exception {
        byte[] data  = tasksAvailableQueue.poll();
        if(data == null) return null;
        String taskName = new String(data);
        Node childNode = tasksDescriptors.getChild(taskName);
        tasksAssigned.createChild(taskName, NodeCreateMode.PERSISTENT);
        DataflowTaskDescriptor descriptor = childNode.getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
        return descriptor;
      }
    };
    return lock.execute(getAssignedtaskOp, 5, 1000);
  }
  
  public void commitFinishedDataflowTaskDescriptor(final DataflowTaskDescriptor descriptor) throws RegistryException {
    Lock lock = tasksLock.getLock("write") ;
    LockOperation<Boolean> commitOp = new LockOperation<Boolean>() {
      @Override
      public Boolean execute() throws Exception {
        Node descriptorNode = registry.get(descriptor.getStoredPath()) ;
        String name = descriptorNode.getName();
        tasksFinished.createChild(name, NodeCreateMode.PERSISTENT);
        tasksAssigned.getChild(name).delete();
        return true;
      }
    };
    lock.execute(commitOp, 5, 1000);
  }
  
  public void suspendDataflowTaskDescriptor(final DataflowTaskDescriptor descriptor) throws RegistryException {
    Lock lock = tasksLock.getLock("write") ;
    LockOperation<Boolean> commitOp = new LockOperation<Boolean>() {
      @Override
      public Boolean execute() throws Exception {
        descriptor.setStatus(Status.SUSPENDED);
        Node descriptorNode = registry.get(descriptor.getStoredPath()) ;
        String name = descriptorNode.getName();
        tasksAvailableQueue.offer(name.getBytes());
        tasksAssigned.getChild(name).delete();
        return true;
      }
    };
    lock.execute(commitOp, 5, 1000);
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
 
  public void update(DataflowTaskDescriptor descriptor) throws RegistryException {
    registry.setData(descriptor.getStoredPath(), descriptor);
  }
 
  
  public void create(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node taskNode = registry.get(descriptor.getStoredPath());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public void update(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node reportNode = registry.get(descriptor.getStoredPath() + "/report");
    reportNode.setData(report);
  }
  
  public List<DataflowTaskDescriptor> getTaskDescriptors() throws RegistryException {
    return tasksDescriptors.getChildrenAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
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
  
  
  
  public List<String> getWorkers() throws RegistryException {
    return workers.getChildren();
  }
  
  public List<DataflowTaskExecutorDescriptor> getExecutors(String worker) throws RegistryException {
    Node executors = workers.getDescendant(worker + "/executors") ;
    return executors.getChildrenAs(DataflowTaskExecutorDescriptor.class);
  }
}
