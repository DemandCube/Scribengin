package com.neverwinterdp.scribengin.dataflow.service;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.registry.task.TaskService;
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
import com.neverwinterdp.util.LoggerFactory;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
@JmxBean("role=dataflow-master, type=DataflowService, dataflowName=DataflowService")
public class DataflowService {
  private Logger logger ;
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
  
  private TaskService<DataflowTaskDescriptor> taskService ;
  
  private DataflowTaskMonitor dataflowTaskMonitor;
  
  private DataflowWorkerMonitor  dataflowWorkerMonitor ;
  
  private DataflowTaskMasterEventListenter masterEventListener ;
  
  private Thread waitForTerminationThread ;
  
  public VMConfig getVMConfig() { return this.vmConfig ; }
  
  public DataflowRegistry getDataflowRegistry() { return dataflowRegistry; }

  public DataflowActivityService getDataflowActivityService() { return this.activityService ;  }
  
  public SourceFactory getSourceFactory() { return sourceFactory; }

  public SinkFactory getSinkFactory() { return sinkFactory; }
  
  @Inject
  public void onInject(LoggerFactory lfactory) {
    logger = lfactory.getLogger(DataflowService.class);
  }
  
  public void addAvailableTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    dataflowRegistry.addAvailableTask(taskDescriptor);
  }
  
  public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    dataflowWorkerMonitor.addWorker(vmDescriptor);
  }
  
  public void run() throws Exception {
    dataflowWorkerMonitor = new DataflowWorkerMonitor(dataflowRegistry, activityService);
    dataflowRegistry.setStatus(DataflowLifecycleStatus.INIT);
    masterEventListener = new DataflowTaskMasterEventListenter(dataflowRegistry);
    
    dataflowTaskMonitor = new DataflowTaskMonitor();
    taskService = new TaskService<>(dataflowRegistry.getTaskRegistry());
    taskService.addTaskMonitor(dataflowTaskMonitor);
    
    activityService.queue(new DataflowInitActivityBuilder().build());
    activityService.queue(new DataflowRunActivityBuilder().build());
  }
  
  
  public void waitForTermination(Thread waitForTerminationThread) throws Exception {
    this.waitForTerminationThread = waitForTerminationThread ;
    dataflowTaskMonitor.waitForAllTaskFinish();
    dataflowWorkerMonitor.waitForAllWorkerTerminated();
    //finish
    dataflowRegistry.setStatus(DataflowLifecycleStatus.FINISH);
  }
  
  public void kill() throws Exception {
    activityService.kill();
    if(waitForTerminationThread != null) {
      waitForTerminationThread.interrupt();
    }
  }
  
  public class DataflowTaskMasterEventListenter extends NodeEventWatcher {
    public DataflowTaskMasterEventListenter(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true/*persistent*/);
      watchModify(dflRegistry.getMasterEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        DataflowEvent taskEvent = getRegistry().getDataAs(event.getPath(), DataflowEvent.class);
        if(taskEvent == DataflowEvent.PAUSE) {
          Activity activity = new DataflowPauseActivityBuilder().build();
          activityService.queue(activity);
          logger.info("Queue a pause activity");
        } else if(taskEvent == DataflowEvent.STOP) {
          activityService.queue(new DataflowStopActivityBuilder().build());
          logger.info("Queue a stop activity");
        } else if(taskEvent == DataflowEvent.RESUME) {
          DataflowLifecycleStatus currentStatus = dataflowRegistry.getStatus();
          if(currentStatus == DataflowLifecycleStatus.PAUSE) {
            Activity activity = new DataflowResumeActivityBuilder().build();
            activityService.queue(activity);
            logger.info("Queue a resume pause activity");
          } else if(currentStatus == DataflowLifecycleStatus.STOP) {
            activityService.queue(new DataflowRunActivityBuilder().build());
            logger.info("Queue a run activity");
          }
          
        }
      }
    }
  }
}
