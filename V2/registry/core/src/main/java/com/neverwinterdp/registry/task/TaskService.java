package com.neverwinterdp.registry.task;

import javax.annotation.PreDestroy;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class TaskService<T>{
  private TaskRegistry<T> taskRegistry ;
  private TaskWatcher<T> taskWatcher ;

  public TaskService() { }
  
  public TaskService(TaskRegistry<T> taskRegistry) throws RegistryException {
    init(taskRegistry) ;
  }
  
  public TaskService(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(new TaskRegistry<T>(registry, path, taskDescriptorType));
  }
  
  protected void init(TaskRegistry<T> taskRegistry) throws RegistryException {
    this.taskRegistry = taskRegistry;
    taskWatcher = new TaskWatcher<T>(taskRegistry) ;
  }
  
  @PreDestroy
  public void onDestroy() {
    taskWatcher.onDestroy();
  }
  
  public TaskRegistry<T> getTaskRegistry() { return taskRegistry; }

  
  public void addTaskMonitor(TaskMonitor<T> monitor) {
    taskWatcher.addTaskMonitor(monitor);
  }
  
  public void offer(String taskId, T taskDescriptor) throws RegistryException {
    taskRegistry.offer(taskId, taskDescriptor);
  }
  
  public TaskContext<T> take(final String executorRefPath) throws RegistryException {
    return taskRegistry.take(executorRefPath);
  }
  
  public void suspend(final String executorRef, TaskTransactionId id) throws RegistryException {
    taskRegistry.suspend(executorRef, id);
  }
  
  public void finish(final String executorRef, TaskTransactionId id) throws RegistryException {
    taskRegistry.finish(executorRef, id);
  }
}