package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;

public class DataflowResumeActivityBuilder extends ActivityBuilder {
  static int idTracker = 1 ;
  
  public DataflowResumeActivityBuilder(DataflowDescriptor dflDescriptor) {
    getActivity().setDescription("Pause Dataflow Activity");
    getActivity().setType("resume-dataflow");
    getActivity().withCoordinator(PauseActivityCoordinator.class);
    add(new ActivityStep().
        withType("broadcast-resume-dataflow-worker").
        withExecutor(BroadcastResumeWorkerStepExecutor.class));
    
    add(new ActivityStep().
        withType("set-dataflow-run-status").
        withExecutor(SetRunningDataflowStatusStepExecutor.class));
  }
  
  @Singleton
  static public class PauseActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityService service, Activity activity, ActivityStep step) {
      activityStepWorkerService.exectute(activity, step);
    }
  }
  
  @Singleton
  static public class BroadcastResumeWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      Node workerNodes = dflRegistry.getActiveWorkersNode() ;
      List<String> workers = workerNodes.getChildren();
      WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(dflRegistry.getRegistry()) ;
      for(int i = 0; i < workers.size(); i++) {
        String path = workerNodes.getPath() + "/" + workers.get(i) + "/status" ;
        waitingListener.add(path, DataflowWorkerStatus.RUNNING);
      }
      
      dflRegistry.broadcastDataflowWorkerEvent(DataflowEvent.RESUME);
      
      waitingListener.waitForEvents(30 * 1000);
      if(waitingListener.getUndetectNodeEventCount() > 0) {
        throw new Exception("Cannot detect the RUNNING status for " + waitingListener.getUndetectNodeEventCount() + " workers") ;
      }
    }
  }
  
  @Singleton
  static public class SetRunningDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    }
  }
}
