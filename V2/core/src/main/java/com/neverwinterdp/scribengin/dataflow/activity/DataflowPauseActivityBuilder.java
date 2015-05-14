package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.util.text.TabularFormater;

public class DataflowPauseActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity() ;
    activity.setDescription("Pause Dataflow Activity");
    activity.setType("pause-dataflow");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowPauseActivityStepBuilder.class);
    return activity;
  }
  
  @Singleton
  static public class DataflowPauseActivityStepBuilder implements ActivityStepBuilder {
    
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(new ActivityStep().
          withType("check-dataflow-status").
          withExecutor(CheckDataflowStatusStepExecutor.class));
      
      steps.add(new ActivityStep().
          withType("broadcast-pause-dataflow-worker").
          withExecutor(BroadcastPauseWorkerStepExecutor.class));
      
      steps.add(new ActivityStep().
          withType("set-dataflow-pause-status").
          withExecutor(SetPauseDataflowStatusStepExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class CheckDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowRegistry dflRegistry ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      if(DataflowLifecycleStatus.RUNNING != dflRegistry.getStatus()) {
       ctx.setAbort(true);
      }
    }
  }
  
  @Singleton
  static public class BroadcastPauseWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      Node workerNodes = dflRegistry.getActiveWorkersNode() ;
      List<String> workers = workerNodes.getChildren();
      WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(dflRegistry.getRegistry()) ;
      for(int i = 0; i < workers.size(); i++) {
        String path = workerNodes.getPath() + "/" + workers.get(i) + "/status" ;
        waitingListener.add(path, DataflowWorkerStatus.PAUSE, "Wait for status PAUSE on " + workers.get(i));
      }
      
      dflRegistry.broadcastWorkerEvent(DataflowEvent.PAUSE);
      
      waitingListener.waitForEvents(30 * 1000);
    }
  }
  
  @Singleton
  static public class SetPauseDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.PAUSE);
    }
  }
}
