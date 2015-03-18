package com.neverwinterdp.scribengin.dataflow.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.event.VMWaitingEventListener;

@Singleton
@JmxBean("role=dataflow-master, type=DataflowService, name=DataflowService")
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
  
  private AssignedDataflowTaskListener assignedDataflowTaskListener;
  
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
    
    assignedDataflowTaskListener = new AssignedDataflowTaskListener(dataflowRegistry);
    
    new DataflowServiceInititializer().onInit(this);
  }
  
  void onRunning() throws Exception {
    System.err.println("onRunning.....................");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    workerListener.waitForEvents(5 * 60 * 1000);
  }
  
  void onFinish() throws Exception {
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
}
