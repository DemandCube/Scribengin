package com.neverwinterdp.scribengin.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.neverwinterdp.registry.DataMapperCallback;
import com.neverwinterdp.registry.MultiDataGet;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.activity.ActivityRegistry;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.registry.task.TaskRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
public class DataflowRegistry {
  final static public String MASTER_EVENT_PATH      = "event/master" ;
  final static public String WORKER_EVENT_PATH      = "event/worker" ;
  final static public String FAILURE_EVENT_PATH     = "event/failure" ;
  
  final static public String ACTIVITIES_PATH        = "activities";
  
  final static public String NOTIFICATIONS_PATH     = "notifications";
  
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
  
  @Inject
  private VMDescriptor       vmDescriptor;
  
  private Node               status;

  private Node               workerEventNode;
  private Node               failureEventNode;
  private Node               masterEventNode;
  
  private Node               activeActivitiesNode;
  
  private Node               allWorkers;
  private Node               activeWorkers;
  private Node               historyWorkers;
  
  private Notifier           dataflowTaskNotifier ;
  private Notifier           dataflowWorkerNotifier ;

  private TaskRegistry<DataflowTaskDescriptor> taskRegistry ;
  
  public DataflowRegistry() { }
  
  public DataflowRegistry(Registry registry, String dataflowPath) throws Exception { 
    this.registry = registry;
    this.dataflowPath = dataflowPath;
    onInit();
  }
  
  @Inject
  public void onInit() throws Exception {
    registry.createIfNotExist(dataflowPath + "/" + MASTER_LEADER_PATH);
    String taskPath = dataflowPath + "/tasks";
    taskRegistry = new TaskRegistry<DataflowTaskDescriptor>(registry, taskPath, DataflowTaskDescriptor.class);
    
    status = registry.createIfNotExist(dataflowPath + "/status");
    
    masterEventNode  = registry.createIfNotExist(dataflowPath + "/" + MASTER_EVENT_PATH);
    workerEventNode  = registry.createIfNotExist(dataflowPath + "/" + WORKER_EVENT_PATH);
    failureEventNode = registry.createIfNotExist(dataflowPath + "/" + FAILURE_EVENT_PATH);
    
    activeActivitiesNode = registry.createIfNotExist(dataflowPath + "/" + ACTIVITIES_PATH);
    
    allWorkers = registry.createIfNotExist(dataflowPath + "/" + ALL_WORKERS_PATH);
    activeWorkers = registry.createIfNotExist(dataflowPath + "/" + ACTIVE_WORKERS_PATH);
    historyWorkers = registry.createIfNotExist(dataflowPath + "/" + HISTORY_WORKERS_PATH);
    
    String notificationPath = getDataflowNotificationsPath() ;
    dataflowTaskNotifier = new Notifier(registry, notificationPath, "dataflow-tasks");
    dataflowWorkerNotifier = new Notifier(registry, notificationPath, "dataflow-workers");
  }
  
  public String getDataflowPath() { return this.dataflowPath ; }
  
  public String getDataflowNotificationsPath() { return this.dataflowPath  + "/" + NOTIFICATIONS_PATH; }
  
  public Node getWorkerEventNode() { return workerEventNode ; }

  public Node getMasterEventNode() { return masterEventNode ; }
  
  public Node getFailureEventNode() { return failureEventNode ; }
  
  public Node getTasksFinishedNode() { return taskRegistry.getTasksFinishedNode();}
  
  public Node getTasksAssignedNode() { return taskRegistry.getTasksAssignedNode(); }
  
  public Node getTasksAssignedHeartbeatNode() { return taskRegistry.getTasksAssignedHeartbeatNode(); }
  
  public Node getActiveActivitiesNode() { return activeActivitiesNode; } 
  
  public Notifier getDataflowTaskNotifier() { return this.dataflowTaskNotifier ; }
  
  public Notifier getDataflowWorkerNotifier() { return this.dataflowWorkerNotifier ; }
  
  public Registry getRegistry() { return this.registry ; }
  
  public TaskRegistry<DataflowTaskDescriptor> getTaskRegistry() { return this.taskRegistry; }
  
  public DataflowDescriptor getDataflowDescriptor() throws RegistryException {
    return registry.getDataAs(dataflowPath, DataflowDescriptor.class);
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

  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    String taskId = taskDescriptor.getTaskId();
    Node taskNode = taskRegistry.getTasksListNode().getChild(taskId);
    taskDescriptor.setRegistryPath(taskNode.getPath());
    taskRegistry.offer(taskId, taskDescriptor);
    create(taskDescriptor, new DataflowTaskReport(taskDescriptor.getTaskId()));
  }

  public TaskContext<DataflowTaskDescriptor> assignDataflowTask(final VMDescriptor vmDescriptor) throws RegistryException  {
    TaskContext<DataflowTaskDescriptor> tContext = taskRegistry.take(vmDescriptor.getRegistryPath());
    return tContext;
  }
  
  public void dataflowTaskSuspend(final TaskContext<DataflowTaskDescriptor> context) throws RegistryException {
    dataflowTaskSuspend(context, false) ;
  }
  
  public void dataflowTaskSuspend(final TaskContext<DataflowTaskDescriptor> context, final boolean disconnectHeartbeat) throws RegistryException {
    taskRegistry.suspend(vmDescriptor.getRegistryPath(), context.getTaskTransactionId(), disconnectHeartbeat);
  }

  public void dataflowTaskFinish(final TaskContext<DataflowTaskDescriptor> context) throws RegistryException {
    taskRegistry.finish(vmDescriptor.getRegistryPath(), context.getTaskTransactionId());
  }
  
  public void dataflowTaskReport(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node  reportNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId()).getChild("report");
    reportNode.setData(report);
  }
  
  public void create(DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws RegistryException {
    Node taskNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId());
    taskNode.createChild("report", report, NodeCreateMode.PERSISTENT);
  }
  
  public List<DataflowTaskDescriptor> getTaskDescriptors() throws RegistryException {
    return taskRegistry.getTasksListNode().getChildrenAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskDescriptor getTaskDescriptor(String taskName) throws RegistryException {
    return taskRegistry.getTasksListNode().getChild(taskName).getDataAs(DataflowTaskDescriptor.class, TASK_DESCRIPTOR_DATA_MAPPER);
  }
  
  public DataflowTaskReport getTaskReport(DataflowTaskDescriptor descriptor) throws RegistryException {
    Node taskNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId());
    return registry.getDataAs(taskNode.getPath() + "/report", DataflowTaskReport.class) ;
  }
  
  public List<DataflowTaskReport> getTaskReports(List<DataflowTaskDescriptor> descriptors) throws RegistryException {
    List<String> reportPaths = new ArrayList<String>();
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowTaskDescriptor descriptor = descriptors.get(i);
      Node taskNode = taskRegistry.getTasksListNode().getChild(descriptor.getTaskId());
      reportPaths.add(taskNode.getPath() + "/report") ;
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
    return activeWorkers.getChildren().size();
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
  
  public void dump() throws RegistryException, IOException {
    registry.get(dataflowPath).dump(System.out);
  }

  public ActivityRegistry getActivityRegistry() throws RegistryException {
    return new ActivityRegistry(registry, dataflowPath + "/" + ACTIVITIES_PATH) ;
  }
  
  static  public DataflowLifecycleStatus getStatus(Registry registry, String dataflowPath) throws RegistryException {
    return registry.getDataAs(dataflowPath + "/status" , DataflowLifecycleStatus.class) ;
  }
  
  static public List<DataflowDescriptor> getDataflowDescriptors(Registry registry) throws RegistryException {
    Node dataflowsNode = registry.get(ScribenginService.DATAFLOWS_PATH) ;
    return dataflowsNode.getChildrenAs(DataflowDescriptor.class) ;
  }
  
  static public List<DataflowTaskDescriptor> getDataflowTaskDescriptors(Registry registry, String dataflowPath) throws RegistryException {
    MultiDataGet<DataflowTaskDescriptor> multiGet = registry.createMultiDataGet(DataflowTaskDescriptor.class);
    multiGet.getChildren(dataflowPath + "/tasks/task-list");
    multiGet.waitForAllGet(30000);
    return multiGet.getResults();
  }
  
  static public List<DataflowTaskReport> getDataflowTaskReports(Registry registry, String dataflowPath) throws RegistryException {
    MultiDataGet<DataflowTaskReport> multiGet = registry.createMultiDataGet(DataflowTaskReport.class);
    String taskListPath = dataflowPath + "/tasks/task-list";
    List<String> taskIds = registry.getChildren(taskListPath) ;
    List<String> reportPaths = new ArrayList<String>() ;
    for(String selTaskId : taskIds) {
      reportPaths.add(taskListPath + "/" + selTaskId + "/report") ;
    }
    multiGet.get(reportPaths);
    multiGet.waitForAllGet(30000);
    return multiGet.getResults();
  }
}