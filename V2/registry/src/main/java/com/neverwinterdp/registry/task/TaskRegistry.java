package com.neverwinterdp.registry.task;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.lock.Lock;
import com.neverwinterdp.registry.notification.Notifier;

public class TaskRegistry<T>{
  final static public String TASK_LIST_PATH          = "task-list";
  final static public String AVAILABLE_PATH          = "executions/available";
  final static public String ASSIGNED_PATH           = "executions/assigned/task-ids";
  final static public String ASSIGNED_HEARTBEAT_PATH = "executions/assigned/task-heartbeats";
  final static public String FINISHED_PATH           = "executions/finished";
  final static public String LOCK_PATH               = "executions/lock";
  final static public String NOTIFICATIONS_PATH      = "notifications";
  
  final static public String TASK_STATUS_PATH        = "status";
  
  private Registry registry ;
  private String   path ;
  private Class<T> taskDescriptorType;
  
  private Node     tasksRootNode ;
  private Node     tasksListNode ;
  private Node     tasksAvailableNode ;
  private Node     tasksAssignedNode ;
  private Node     tasksAssignedHeartbeatNode ;
  private Node     tasksFinishedNode ;
  private Node     tasksLockNode ; 
  
  private Notifier taskExecutionNotifier ;
  private Notifier taskCoordinationNotifier ;

  public TaskRegistry() { }
  
  public TaskRegistry(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    this.registry = registry;
    this.path     = path;
    this.taskDescriptorType = taskDescriptorType;
    
    this.tasksRootNode = registry.createIfNotExist(path) ;
    this.tasksListNode = tasksRootNode.createDescendantIfNotExists( TASK_LIST_PATH); 
    this.tasksAvailableNode = tasksRootNode.createDescendantIfNotExists(AVAILABLE_PATH);
    this.tasksAssignedNode = tasksRootNode.createDescendantIfNotExists(ASSIGNED_PATH);
    this.tasksAssignedHeartbeatNode = tasksRootNode.createDescendantIfNotExists(ASSIGNED_HEARTBEAT_PATH);
    this.tasksFinishedNode = tasksRootNode.createDescendantIfNotExists(FINISHED_PATH);
    this.tasksLockNode = tasksRootNode.createDescendantIfNotExists(LOCK_PATH);
    
    taskExecutionNotifier = new Notifier(registry, path + "/" + NOTIFICATIONS_PATH, "task-execution");
    taskCoordinationNotifier = new Notifier(registry, path + "/" + NOTIFICATIONS_PATH, "task-coordination");
  }
  
  public Registry getRegistry() { return registry; }

  public String getPath() { return path; }
  
  public Node getTasksRootNode() { return tasksRootNode; }
  
  public Node getTasksListNode() { return tasksListNode; }

  public Node getTasksAvailableNode() { return tasksAvailableNode; }

  public Node getTasksAssignedNode() { return tasksAssignedNode; }
  
  public Node getTasksAssignedHeartbeatNode() { return tasksAssignedHeartbeatNode; }
  
  public Node getTasksFinishedNode() { return tasksFinishedNode; }

  public Notifier getTaskExecutionNotifier() { return this.taskExecutionNotifier ; }
  
  public Notifier getTaskCoordinationNotifier() { return this.taskCoordinationNotifier ; }
  
  public T getTaskDescriptor(String taskId) throws RegistryException {
    return tasksListNode.getChild(taskId).getDataAs(taskDescriptorType) ;
  }
  
  public TaskStatus getTaskStatus(String taskId) throws RegistryException {
    return tasksListNode.getChild(taskId).getChild(TASK_STATUS_PATH).getDataAs(TaskStatus.class) ;
  }
  
  public void offer(String taskId, T taskDescriptor) throws RegistryException {
    Transaction transaction = registry.getTransaction() ;
    transaction.createChild(tasksListNode, taskId, taskDescriptor, NodeCreateMode.PERSISTENT);
    transaction.createDescendant(tasksListNode, taskId + "/" + TASK_STATUS_PATH, TaskStatus.INIT, NodeCreateMode.PERSISTENT);
    transaction.createChild(tasksAvailableNode, taskId, NodeCreateMode.PERSISTENT);
    transaction.commit();
  }
  
  public TaskContext<T> take(final String executorRefPath) throws RegistryException {
    BatchOperations<TaskContext<T>> takeOp = new BatchOperations<TaskContext<T>>() {
      @Override
      public TaskContext<T> execute(Registry registry) throws RegistryException {
        List<String> availableTasks = tasksAvailableNode.getChildren() ;
        if(availableTasks.size() == 0) return null ;
        Collections.sort(availableTasks);
        String taskId = availableTasks.get(0) ;
        try {
          Node taskNode = tasksListNode.getChild(taskId) ;
          Transaction transaction = registry.getTransaction();
          TaskTransactionId taskTransactionID = new TaskTransactionId(taskId, Math.abs(transaction.hashCode()) + "");
          
          transaction.setData(taskNode.getChild(TASK_STATUS_PATH), TaskStatus.PROCESSING);
          transaction.createChild(tasksAssignedNode, taskTransactionID.getTaskTransactionId(), NodeCreateMode.PERSISTENT);
          transaction.createChild(tasksAssignedHeartbeatNode, taskTransactionID.getTaskTransactionId(), new RefNode(executorRefPath), NodeCreateMode.EPHEMERAL);
          transaction.deleteChild(tasksAvailableNode, taskId);
          transaction.commit();
          TaskContext<T> taskContext = createTaskContext(taskTransactionID, null) ;
          return taskContext;
        } catch(Exception ex) {
          String errorMessage = "Fail to grab task " + taskId + " for the executor " + executorRefPath;
          StringBuilder registryDump = new StringBuilder() ;
          try {
            tasksAssignedNode.getParentNode().dump(registryDump);
          } catch (IOException e) {
          }
          errorMessage += "\n" + registryDump.toString();
          taskExecutionNotifier.warn("fail-to-grab-a-task ", errorMessage, ex);
          throw ex;
        }
      }
    };
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to grab a task for the executor " + executorRefPath) ;
      return lock.execute(takeOp, 3, 3000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to assign the task after 3 tries";
      taskExecutionNotifier.error("fail-to-grab-a-task", errorMessage, ex);
      throw ex;
    }
  }
  
  public void suspend(final String executorRef, TaskTransactionId taskTransactionID) throws RegistryException {
    suspend(executorRef, taskTransactionID, false);
  }
  
  public void suspend(final String executorRef, final TaskTransactionId taskTransactionID, final boolean disconnectHeartbeat) throws RegistryException {
    BatchOperations<Boolean> suspendtOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Node taskNode = tasksListNode.getChild(taskTransactionID.getTaskId()) ;
          Transaction transaction = registry.getTransaction();
          transaction.setData(taskNode.getChild(TASK_STATUS_PATH), TaskStatus.SUSPENDED);
          transaction.deleteChild(tasksAssignedNode, taskTransactionID.getTaskTransactionId()) ;
          if(!disconnectHeartbeat) {
            transaction.deleteChild(tasksAssignedHeartbeatNode, taskTransactionID.getTaskTransactionId()) ;
          }
          transaction.createChild(tasksAvailableNode, taskTransactionID.getTaskId(), NodeCreateMode.PERSISTENT) ;
          transaction.commit();
          return true;
        } catch(RegistryException ex) {
          String errorMessage = "Fail to suspend the task " + taskTransactionID.getTaskTransactionId();
          StringBuilder registryDump = new StringBuilder() ;
          try {
            tasksAssignedNode.getParentNode().dump(registryDump);
          } catch (IOException e) {
          }
          errorMessage += "\n" + registryDump.toString();
          taskExecutionNotifier.warn("fail-to-suspend-dataflow-task", errorMessage, ex);
          throw ex ;
        }
      }
    };
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to move the task " + taskTransactionID.getTaskTransactionId() + " to suspend by " + executorRef) ;
      lock.execute(suspendtOp, 3, 3000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to suspend the task " + taskTransactionID.getTaskTransactionId();
      taskExecutionNotifier.error("fail-to-suspend-dataflow-task", errorMessage, ex);
      throw ex;
    }
  }
  
  public void finish(final String executorRef, final TaskTransactionId taskTransactionID) throws RegistryException {
    BatchOperations<Boolean> commitOp = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        try {
          Node taskNode = tasksListNode.getChild(taskTransactionID.getTaskId()) ;
          Transaction transaction = registry.getTransaction();
          //update the task descriptor
          transaction.setData(taskNode.getChild(TASK_STATUS_PATH), TaskStatus.TERMINATED);
          transaction.createChild(tasksFinishedNode, taskTransactionID.getTaskId(), NodeCreateMode.PERSISTENT);
          transaction.deleteChild(tasksAssignedNode, taskTransactionID.getTaskTransactionId());
          transaction.deleteChild(tasksAssignedHeartbeatNode, taskTransactionID.getTaskTransactionId());
          transaction.commit();
          return true;
        } catch(RegistryException ex) {
          String errorMessage = "Fail to finish the task " + taskTransactionID.getTaskTransactionId();
          taskExecutionNotifier.warn("fail-to-finish-a-task", errorMessage, ex);
          throw ex;
        }
      }
    };
    try {
      Lock lock = tasksLockNode.getLock("write", "Lock to move the task " + taskTransactionID.getTaskTransactionId() + " to finish by " + executorRef) ;
      lock.execute(commitOp, 3, 3000);
    } catch(RegistryException ex) {
      String errorMessage = "Fail to finish the task " + taskTransactionID.getTaskTransactionId();
      taskExecutionNotifier.warn("fail-to-finish-a-task", errorMessage, ex);
      throw ex;
    }
  }
  
  public TaskContext<T> createTaskContext(String taskTransactionId) throws RegistryException {
    return createTaskContext(new TaskTransactionId(taskTransactionId), null) ;
  }
  
  public TaskContext<T> createTaskContext(TaskTransactionId taskTransactionId, T taskDescriptor) throws RegistryException {
    TaskContext<T> taskContext = new TaskContext<T>(this, taskTransactionId, taskDescriptor);
    return taskContext;
  }
}