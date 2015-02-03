package com.neverwinterdp.scribengin.dataflow.worker;

import java.io.IOException;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowTaskExecutor {
  static public enum InterruptEvent { SwitchTask, Exit } ;
  
  private DataflowTaskExecutorDescriptor descriptor;
  private DataflowContainer dataflowContainer ;
  private InterruptEvent interruptEvent = InterruptEvent.Exit;
  private ExecutorThread executorThread ;

  public DataflowTaskExecutor(DataflowTaskExecutorDescriptor  descriptor, DataflowContainer container) throws RegistryException {
    this.descriptor = descriptor;
    this.dataflowContainer = container;
    DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry() ;
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    dataflowRegistry.createTaskExecutor(vmDescriptor, descriptor);
  }
  
  public void start() {
    executorThread = new ExecutorThread();
    executorThread.start();
  }
  
  public void interrupt(InterruptEvent event) {
    this.interruptEvent = event;
    executorThread.interrupt();
  }

  public boolean isAlive() {
    if(executorThread == null) return false;
    return executorThread.isAlive();
  }
  
  public void execute() { 
    descriptor.setStatus(DataflowTaskExecutorDescriptor.Status.RUNNING);
    DataflowTask dataflowTask = null;
    VMDescriptor vmDescriptor = dataflowContainer.getVMDescriptor() ;
    while(true) {
      try {
        DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry();
        DataflowTaskDescriptor taskDescriptor = dataflowRegistry.getAssignedDataflowTaskDescriptor();
        if(taskDescriptor == null) {
          descriptor.setStatus(DataflowTaskExecutorDescriptor.Status.TERMINATED);
          dataflowRegistry.updateTaskExecutor(vmDescriptor, descriptor);
          return;
        }
        descriptor.addAssignedTask(taskDescriptor);
        dataflowRegistry.updateTaskExecutor(vmDescriptor, descriptor);
        dataflowTask = new DataflowTask(dataflowContainer, taskDescriptor);
        dataflowTask.execute();
        dataflowRegistry.commitFinishedDataflowTaskDescriptor(taskDescriptor);
      } catch (InterruptedException e) {
        if(interruptEvent == InterruptEvent.SwitchTask) {
          interruptEvent = InterruptEvent.Exit;
        } else {
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
        Registry registry = dataflowContainer.getDataflowRegistry().getRegistry() ;
        try {
          StringBuilder b = new StringBuilder() ;
          b.append("Executor Error and lock dumps\n");
          registry.get(dataflowContainer.getDataflowRegistry().getDataflowPath() + "/tasks/locks").dump(b);
          System.err.println(b);
        } catch (RegistryException | IOException e1) {
          e1.printStackTrace();
        }
        e.printStackTrace();
      }
    }
  }
  
  public class ExecutorThread extends Thread {
    public void run() {
      execute();
    }
  }
}