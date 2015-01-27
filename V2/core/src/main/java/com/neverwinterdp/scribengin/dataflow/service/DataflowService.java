package com.neverwinterdp.scribengin.dataflow.service;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.SourceFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.event.VMWaitingEventListener;


public class DataflowService {
  @Inject
  private VMConfig vmConfig;
 
  @Inject
  private DataflowRegistry dataflowRegistry;
  
  @Inject
  private SourceFactory sourceFactory ;
  
  @Inject
  private SinkFactory sinkFactory ;
  
  private VMWaitingEventListener workerListener ;
  
  private List<DataflowServiceEventListener> listeners = new ArrayList<DataflowServiceEventListener>();
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    dataflowRegistry.addAvailableTask(taskDescriptor);
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    dataflowRegistry.addWorker(vmDescriptor);
    workerListener.waitHeartbeat("Wait for " + vmDescriptor.getId(), vmDescriptor.getId(), false);
  }
  
  public void run() throws Exception {
    onInit() ;
    onRunning() ;
    onFinish();
  }
  
  void onInit() throws Exception {
    System.err.println("onInit.....................");
    workerListener = new VMWaitingEventListener(dataflowRegistry.getRegistry());
    dataflowRegistry.setStatus(DataflowLifecycleStatus.INIT);
    listeners.add(new DataflowServiceInitEventListener());
    onEvent(DataflowLifecycleStatus.INIT);
  }
  
  void onRunning() throws Exception {
    System.err.println("onRunning.....................");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    workerListener.waitForEvents(5 * 60 * 1000);
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
}
