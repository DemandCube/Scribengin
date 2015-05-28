package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class DataflowTaskExecutor {
  private DataflowTaskExecutorDescriptor executorDescriptor;
  private DataflowContainer              dataflowContainer;
  private ExecutorManagerThread          executorManagerThread;
  private DataflowTaskExecutorThread     executorThread;
  private MetricRegistry                 metricRegistry;
  private DataflowTask                   currentDataflowTask = null;
  private boolean                        interrupt           = false;
  private boolean                        kill                = false;

  public DataflowTaskExecutor(DataflowTaskExecutorDescriptor  descriptor, DataflowContainer container) throws RegistryException {
    executorDescriptor = descriptor;
    dataflowContainer = container;
    DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry() ;
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    metricRegistry = dataflowContainer.getInstance(MetricRegistry.class);
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
    Timer dataflowTaskTimerGrab = metricRegistry.getTimer("dataflow-task.timer.grab") ;
    Timer dataflowTaskTimerProcess = metricRegistry.getTimer("dataflow-task.timer.process") ;
    try {
      while(!interrupt) {
        Timer.Context dataflowTaskTimerGrabCtx = dataflowTaskTimerGrab.time() ;
        
        TaskContext<DataflowTaskDescriptor> taskContext= dataflowRegistry.assignDataflowTask(vmDescriptor);
        dataflowTaskTimerGrabCtx.stop();
        
        if(interrupt) return ;
        if(taskContext == null) return;
        
        Timer.Context dataflowTaskTimerProcessCtx = dataflowTaskTimerProcess.time() ;
        executorDescriptor.addAssignedTask(taskContext.getTaskTransactionId().getTaskId());
        dataflowRegistry.updateWorkerTaskExecutor(vmDescriptor, executorDescriptor);
        currentDataflowTask = new DataflowTask(dataflowContainer, taskContext);
        currentDataflowTask.init();
        executorThread = new DataflowTaskExecutorThread(currentDataflowTask);
        executorThread.start();
        executorThread.waitForTimeout(10000);
        if(currentDataflowTask.isComplete()) currentDataflowTask.finish();
        else currentDataflowTask.suspend();
        dataflowTaskTimerProcessCtx.stop();
      }
    } catch (InterruptedException e) {
      System.err.println("detect shutdown interrupt for task " + currentDataflowTask.getDescriptor().getTaskId());
      currentDataflowTask.interrupt();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      doExit();
    }
  }

  void doExit() {
    if(kill) return ;
    try {
      DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry();
      VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
      executorDescriptor.setStatus(DataflowTaskExecutorDescriptor.Status.TERMINATED);
      dataflowRegistry.updateWorkerTaskExecutor(vmDescriptor, executorDescriptor);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  /**
   * This method is used to simulate the failure
   * @throws Exception
   */
  public void kill() throws Exception {
    kill = true;
    if(executorThread != null && executorThread.isAlive()) executorThread.interrupt();
    if(executorManagerThread != null && executorManagerThread.isAlive()) executorManagerThread.interrupt();
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