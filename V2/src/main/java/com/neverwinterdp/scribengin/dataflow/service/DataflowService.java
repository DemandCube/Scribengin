package com.neverwinterdp.scribengin.dataflow.service;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceFactory;
import com.neverwinterdp.vm.VMConfig;


public class DataflowService {
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dataflowRegistry;
  
  @Inject
  private SourceFactory sourceFactory ;
  
  @Inject
  private SinkFactory sinkFactory ;
  
  private List<DataflowServiceEventListener> listeners = new ArrayList<DataflowServiceEventListener>();
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  public void run() throws Exception {
    onInit() ;
    onRunning() ;
    onFinish();
  }
  
  void onInit() throws Exception {
    System.err.println("onInit.....................");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.INIT);
    listeners.add(new DataflowServiceInitEventListener());
    onEvent(DataflowLifecycleStatus.INIT);
  }
  
  void onRunning() throws Exception {
    System.err.println("onRunning.....................");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    String tasksFinishedPath = dataflowRegistry.getTasksFinishedPath();
    DataflowDescriptor descriptor = dataflowRegistry.getDataflowDescriptor();
    DataflowFinishTaskWatcher finishTaskWatcher = new DataflowFinishTaskWatcher();
    int finishTasks = 0;
    while(finishTasks < 15) {
      dataflowRegistry.getRegistry().watchChildren(tasksFinishedPath, finishTaskWatcher);
      int newFinishTasks = finishTaskWatcher.waitForFinishedTasks(10000);
      if(finishTasks == newFinishTasks) {
        newFinishTasks = dataflowRegistry.getRegistry().getChildren(tasksFinishedPath).size() ;
      }
      finishTasks = newFinishTasks;
      System.err.println("finished task: " + finishTasks);
    }
  }
  
  void onFinish() throws Exception {
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
  
  private void onEvent(DataflowLifecycleStatus event) throws Exception {
    for(int i = 0; i < listeners.size(); i++) {
      DataflowServiceEventListener listener = listeners.get(i) ;
      listener.onEvent(this, event);
    }
  }
  
  public class DataflowFinishTaskWatcher extends NodeWatcher {
    private int finishTaskCount = 0;
    
    @Override
    public void process(NodeEvent event) {
      try {
        List<String> children = dataflowRegistry.getRegistry().getChildren(event.getPath());
        finishTaskCount = children.size();
        synchronized(this) {
          notifyAll();
        }
      } catch (RegistryException e) {
        e.printStackTrace();
      }
    }
    
    synchronized public int waitForFinishedTasks(long timeout) throws InterruptedException {
      wait(timeout);
      return finishTaskCount;
    }
    
  }
}
