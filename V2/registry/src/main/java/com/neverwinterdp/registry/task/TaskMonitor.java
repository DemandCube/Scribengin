package com.neverwinterdp.registry.task;

public interface TaskMonitor<T> {
  public void onAssign(TaskContext<T> context) ;
  public void onAvailable(TaskContext<T> context) ;
  public void onFinish(TaskContext<T> context) ;
}
