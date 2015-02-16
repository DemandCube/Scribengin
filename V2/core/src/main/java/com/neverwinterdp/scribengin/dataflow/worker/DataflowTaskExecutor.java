package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowTaskExecutor {
  private DataflowTaskExecutorDescriptor executorDescriptor;
  private DataflowContainer dataflowContainer ;
  private ExecutorManagerThread executorManagerThread ;

  public DataflowTaskExecutor(DataflowTaskExecutorDescriptor  descriptor, DataflowContainer container) throws RegistryException {
    executorDescriptor = descriptor;
    dataflowContainer = container;
    DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry() ;
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    dataflowRegistry.createTaskExecutor(vmDescriptor, descriptor);
  }
  
  public DataflowTaskExecutorDescriptor getDescriptor() { return this.executorDescriptor ; }
  
  public void start() {
    executorManagerThread = new ExecutorManagerThread();
    executorManagerThread.start();
  }
  
  public void shutdown() throws Exception {
    if(isAlive()) executorManagerThread.interrupt();
  }
  
  public boolean isAlive() {
    if(executorManagerThread == null) return false;
    return executorManagerThread.isAlive();
  }
  
  public void execute() { 
    executorDescriptor.setStatus(DataflowTaskExecutorDescriptor.Status.RUNNING);
    DataflowTask dataflowTask = null;
    DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry();
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    try {
      while(true) {
        DataflowTaskDescriptor taskDescriptor = dataflowRegistry.assignDataflowTask(vmDescriptor);
        if(taskDescriptor == null) return;

        executorDescriptor.addAssignedTask(taskDescriptor);
        dataflowRegistry.updateTaskExecutor(vmDescriptor, executorDescriptor);
        dataflowTask = new DataflowTask(dataflowContainer, taskDescriptor);
        dataflowTask.init();
        DataflowTaskExecutorThread executorThread = new DataflowTaskExecutorThread(dataflowTask);
        executorThread.start();
        executorThread.waitForTimeout(10000);
        if(dataflowTask.isComplete()) dataflowTask.finish();
        else dataflowTask.suspend();
      }
    } catch (InterruptedException e) {
      dataflowTask.interrupt();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      doExit();
    }
  }

  void doExit() {
    try {
      DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry();
      VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
      executorDescriptor.setStatus(DataflowTaskExecutorDescriptor.Status.TERMINATED);
      dataflowRegistry.updateTaskExecutor(vmDescriptor, executorDescriptor);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public class ExecutorManagerThread extends Thread {
    public void run() {
      execute();
    }
  }
  
  public class DataflowTaskExecutorThread extends Thread {
    DataflowTask  dataflowtask;
    private boolean terminated = false;
    
    public DataflowTaskExecutorThread(DataflowTask  dataflowtask) {
      this.dataflowtask = dataflowtask;
    }

    public void run() {
      try {
        dataflowtask.run();
        notifyTermination();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
    
    synchronized public void notifyTermination() {
      terminated = true;
      notifyAll() ;
    }
    
    synchronized void waitForTimeout(long timeout) throws InterruptedException {
      wait(timeout);
      if(!terminated) dataflowtask.interrupt();
      waitForTerminated();
    }
    
    synchronized void waitForTerminated() throws InterruptedException {
      if(terminated) return ;
      wait(3000);
    }
  }
}