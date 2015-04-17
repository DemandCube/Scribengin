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
  private DataflowTask currentDataflowTask = null;
  private boolean interrupt = false;
  
  public DataflowTaskExecutor(DataflowTaskExecutorDescriptor  descriptor, DataflowContainer container) throws RegistryException {
    executorDescriptor = descriptor;
    dataflowContainer = container;
    DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry() ;
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    dataflowRegistry.createWorkerTaskExecutor(vmDescriptor, descriptor);
  }
  
  public DataflowTaskExecutorDescriptor getDescriptor() { return this.executorDescriptor ; }
  
  public void start() {
    interrupt = false ;
    executorManagerThread = new ExecutorManagerThread();
    executorManagerThread.start();
  }
  
  public void interrupt() throws Exception {
    if(isAlive()) {
      interrupt = true ;
      if(currentDataflowTask != null) currentDataflowTask.interrupt();
    }
  }
  
  public boolean isAlive() {
    if(executorManagerThread == null) return false;
    return executorManagerThread.isAlive();
  }
  
  public void execute() { 
    executorDescriptor.setStatus(DataflowTaskExecutorDescriptor.Status.RUNNING);
    DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry();
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    try {
      while(!interrupt) {
        DataflowTaskDescriptor taskDescriptor = dataflowRegistry.assignDataflowTask(vmDescriptor);
        if(interrupt) return ;
        if(taskDescriptor == null) return;
        
        executorDescriptor.addAssignedTask(taskDescriptor);
        dataflowRegistry.updateWorkerTaskExecutor(vmDescriptor, executorDescriptor);
        currentDataflowTask = new DataflowTask(dataflowContainer, taskDescriptor);
        currentDataflowTask.init();
        DataflowTaskExecutorThread executorThread = new DataflowTaskExecutorThread(currentDataflowTask);
        executorThread.start();
        executorThread.waitForTimeout(10000);
        if(currentDataflowTask.isComplete()) currentDataflowTask.finish();
        else currentDataflowTask.suspend();
      }
    } catch (InterruptedException e) {
      System.err.println("detect shutdown interrupt for task " + currentDataflowTask.getDescriptor().getId());
      currentDataflowTask.interrupt();
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
      dataflowRegistry.updateWorkerTaskExecutor(vmDescriptor, executorDescriptor);
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