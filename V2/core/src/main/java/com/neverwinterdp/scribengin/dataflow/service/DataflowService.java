package com.neverwinterdp.scribengin.dataflow.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.SequenceNumberTrackerService;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowInitActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowPauseActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowResumeActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowRunActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowStopActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
@JmxBean("role=dataflow-master, type=DataflowService, dataflowName=DataflowService")
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
  
  private DataflowWorkerMonitor  dataflowWorkerMonitor ;
  
  private DataflowTaskMasterEventListenter masterEventListener ;
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public DataflowActivityService getDataflowActivityService() { return this.activityService ;  }
  
  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    dataflowRegistry.addAvailableTask(taskDescriptor);
    dataflowTaskMonitor.addMonitorTask(taskDescriptor);
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    dataflowWorkerMonitor.addWorker(vmDescriptor);
  }
  
  public void run() throws Exception {
    dataflowWorkerMonitor = new DataflowWorkerMonitor(dataflowRegistry, activityService);
    dataflowRegistry.setStatus(DataflowLifecycleStatus.INIT);
    masterEventListener = new DataflowTaskMasterEventListenter(dataflowRegistry);
    dataflowTaskMonitor = new DataflowTaskMonitor(dataflowRegistry);
    
    activityService.queue(new DataflowInitActivityBuilder().build());
    activityService.queue(new DataflowRunActivityBuilder().build());
  }
  
  
  public void waitForTermination() throws Exception {
    System.err.println("DataflowService: waitForTermination()");
    dataflowTaskMonitor.waitForAllTaskFinish();
    dataflowWorkerMonitor.waitForAllWorkerTerminated();
    //finish
    System.err.println("DataflowService: FINISH");
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
  
  public class DataflowTaskMasterEventListenter extends NodeEventWatcher {
    public DataflowTaskMasterEventListenter(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true/*persistent*/);
      watchModify(dflRegistry.getDataflowTasksMasterEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        DataflowEvent taskEvent = getRegistry().getDataAs(event.getPath(), DataflowEvent.class);
        if(taskEvent == DataflowEvent.PAUSE) {
          DataflowPauseActivityBuilder dataflowPauseActivityBuilder = 
              new DataflowPauseActivityBuilder(dataflowRegistry.getDataflowDescriptor());
          ActivityCoordinator pauseCoordinator = activityService.start(dataflowPauseActivityBuilder);
        } else if(taskEvent == DataflowEvent.STOP) {
          activityService.queue(new DataflowStopActivityBuilder().build());
        } else if(taskEvent == DataflowEvent.RESUME) {
          DataflowLifecycleStatus currentStatus = dataflowRegistry.getStatus();
          System.err.println("Detect the resume event, current status = " + currentStatus);
          if(currentStatus == DataflowLifecycleStatus.PAUSE) {
            System.err.println("  Run resume activity");
            DataflowResumeActivityBuilder dataflowResumeActivityBuilder = 
                new DataflowResumeActivityBuilder(dataflowRegistry.getDataflowDescriptor());
            ActivityCoordinator resumeCoordinator = activityService.start(dataflowResumeActivityBuilder);
          } else if(currentStatus == DataflowLifecycleStatus.STOP) {
            System.err.println("  Run run activity...");
            activityService.queue(new DataflowRunActivityBuilder().build());
          }
          
        }
      }
      System.out.println("event = " + event.getType() + ", path = " + event.getPath());
    }
  }
}
