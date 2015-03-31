package com.neverwinterdp.scribengin.dataflow.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowInitActivityBuilder;
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
  
  @Inject
  private DataflowActivityService activityService;
  
  private VMWaitingEventListener workerListener ;
  
  private AssignedDataflowTaskListener assignedDataflowTaskListener;
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public DataflowActivityService getDataflowActivityService() { return this.activityService ;  }
  
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
    System.err.println("onInit.....................");
    workerListener = new VMWaitingEventListener(dataflowRegistry.getRegistry());
    dataflowRegistry.setStatus(DataflowLifecycleStatus.INIT);
    
    assignedDataflowTaskListener = new AssignedDataflowTaskListener(dataflowRegistry);
    
    DataflowInitActivityBuilder dataflowInitActivityBuilder = new DataflowInitActivityBuilder(dataflowRegistry.getDataflowDescriptor());
    ActivityCoordinator coordinator = activityService.start(dataflowInitActivityBuilder);
    coordinator.waitForTermination(60000);
    
    System.err.println("onRunning.....................");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    workerListener.waitForEvents(5 * 60 * 1000);
    
    //finish
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
}
