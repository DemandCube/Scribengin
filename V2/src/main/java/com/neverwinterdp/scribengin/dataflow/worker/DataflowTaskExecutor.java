package com.neverwinterdp.scribengin.dataflow.worker;

import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTask;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;

public class DataflowTaskExecutor {
  static public enum InterruptEvent { SwitchTask, Exit } ;

  private DataflowContainer dataflowContainer ;
  private InterruptEvent interruptEvent = InterruptEvent.Exit;
  private ExecutorThread executorThread ;

  public DataflowTaskExecutor(DataflowContainer container) {
    this.dataflowContainer = container;
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
    DataflowTask dataflowTask = null;
    while(true) {
      try {
        DataflowRegistry dataflowRegistry = dataflowContainer.getDataflowRegistry();
        DataflowTaskDescriptor descriptor = dataflowRegistry.getAssignedDataflowTaskDescriptor();
        if(descriptor == null) return;
        
        dataflowTask = new DataflowTask(dataflowContainer, descriptor);
        dataflowTask.execute();
        dataflowRegistry.commitFinishedDataflowTaskDescriptor(descriptor);
      } catch (InterruptedException e) {
        if(interruptEvent == InterruptEvent.SwitchTask) {
          interruptEvent = InterruptEvent.Exit;
        } else {
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
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