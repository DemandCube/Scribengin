package com.neverwinterdp.scribengin.dataflow.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskEvent;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowRunActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskWorkerEvent;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

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
  
  private DataflowTaskMonitor dataflowTaskMonitor;
  
  private DataflowTaskMasterEventListenter masterEventListener ;
  
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
  }
  
  public void run() throws Exception {
    dataflowRegistry.setStatus(DataflowLifecycleStatus.INIT);
    masterEventListener = new DataflowTaskMasterEventListenter(dataflowRegistry);
    DataflowInitActivityBuilder dataflowInitActivityBuilder = new DataflowInitActivityBuilder(dataflowRegistry.getDataflowDescriptor());
    ActivityCoordinator coordinator = activityService.start(dataflowInitActivityBuilder);
    coordinator.waitForTermination(60000);
    executeRunActivity();
  }
  
  private void executeRunActivity() throws Exception {
    System.err.println("execute RUN activity");
    
    dataflowTaskMonitor = new DataflowTaskMonitor(dataflowRegistry);
    
    DataflowRunActivityBuilder dataflowInitActivityBuilder = new DataflowRunActivityBuilder(dataflowRegistry.getDataflowDescriptor());
    ActivityCoordinator coordinator = activityService.start(dataflowInitActivityBuilder);
    coordinator.waitForTermination(60000);
    
    System.err.println("DataflowService: RUNNING");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    
    dataflowTaskMonitor.waitForAllTaskFinish();
    stopWorkers();
    //finish
    System.err.println("DataflowService: FINISH");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
  
  public void stopWorkers() throws Exception {
    dataflowRegistry.setDataflowTaskWorkerEvent(DataflowTaskWorkerEvent.STOP);
  }
  
  public void startWorkers() throws Exception {
    DataflowRunActivityBuilder dataflowInitActivityBuilder = 
      new DataflowRunActivityBuilder(dataflowRegistry.getDataflowDescriptor());
    ActivityCoordinator coordinator = activityService.start(dataflowInitActivityBuilder);
    //coordinator.waitForTermination(60000);
  }
  
  public class DataflowTaskMasterEventListenter extends NodeEventWatcher {
    public DataflowTaskMasterEventListenter(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true/*persistent*/);
      watchModify(dflRegistry.getDataflowTasksMasterEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        DataflowTaskEvent taskEvent = getRegistry().getDataAs(event.getPath(), DataflowTaskEvent.class);
        if(taskEvent == DataflowTaskEvent.PAUSE) {
          stopWorkers();
        } else if(taskEvent == DataflowTaskEvent.STOP) {
          stopWorkers();
        } else if(taskEvent == DataflowTaskEvent.RESUME) {
          startWorkers();
        }
      }
      System.out.println("event = " + event.getType() + ", path = " + event.getPath());
    }
  }
}
