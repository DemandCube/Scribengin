package com.neverwinterdp.registry.task;

import javax.annotation.PreDestroy;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class TaskService<T>{
  private TaskRegistry<T> taskRegistry ;
  private TaskWatcher<T> taskWatcher ;

  public TaskService() { }
  
  public TaskService(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    init(registry, path, taskDescriptorType) ;
  }
  
  protected void init(Registry registry, String path, Class<T> taskDescriptorType) throws RegistryException {
    taskRegistry = new TaskRegistry<T>(registry, path, taskDescriptorType);
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
  
  public void suspend(final String executorRef, final String taskId) throws RegistryException {
    taskRegistry.suspend(executorRef, taskId);
  }
  
  public void finish(final String executorRef, final String taskId) throws RegistryException {
    taskRegistry.finish(executorRef, taskId);
  }
}