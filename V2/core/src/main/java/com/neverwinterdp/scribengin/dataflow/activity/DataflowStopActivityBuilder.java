package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;

public class DataflowStopActivityBuilder extends ActivityBuilder {
  public Activity build() {
    Activity activity = new Activity();
    activity.setDescription("Stop Dataflow Activity");
    activity.setType("stop-dataflow");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowStopActivityStepBuilder.class);
    return activity;
  }
  
  @Singleton
  static public class DataflowStopActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(new ActivityStep().
          withType("check-dataflow-status").
          withExecutor(CheckDataflowStatusStepExecutor.class));
      steps.add(new ActivityStep().
          withType("broadcast-stop-dataflow-worker").
          withExecutor(BroadcastStopWorkerStepExecutor.class));
      
      steps.add(new ActivityStep().
          withType("set-dataflow-stop-status").
          withExecutor(SetStopDataflowStatusStepExecutor.class));
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
  static public class BroadcastStopWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      ActiveDataflowWorkerWatcher workerWatcher = new ActiveDataflowWorkerWatcher(dflRegistry, true) ;
      dflRegistry.broadcastWorkerEvent(DataflowEvent.STOP);
      if(!workerWatcher.waitForNoMoreWorker(45 * 1000)) {
       throw new Exception("Wait for no more workers, but there is still " + dflRegistry.countActiveDataflowWorkers() + " workers") ; 
      }
    }
  }
  
  @Singleton
  static public class SetStopDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.STOP);
    }
  }
  
  static public class ActiveDataflowWorkerWatcher extends NodeChildrenWatcher {
    private List<String> activeWorkers = null ;
    private Node activeWorkersNode ;
    
    public ActiveDataflowWorkerWatcher(DataflowRegistry dflRegistry, boolean persistent) throws RegistryException {
      super(dflRegistry.getRegistry(), persistent);
      activeWorkersNode = dflRegistry.getAllWorkersNode() ;
      watchChildren(activeWorkersNode.getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
        activeWorkers = getRegistry().getChildren(event.getPath());
        notifyActiveWorkerChange();
      } else if(event.getType() == NodeEvent.Type.DELETE) {
        setComplete(); ;
      }
    }
    
    synchronized void notifyActiveWorkerChange() {
      notifyAll() ;
    }
    
    synchronized List<String> waitForActiveWorkerChange(long timeout) throws InterruptedException {
      wait(timeout) ;
      return activeWorkers ;
    }
    
    public boolean waitForNoMoreWorker(long timeout) throws Exception, InterruptedException {
      long waitTime = timeout ;
      while(waitTime > 0) {
        long start = System.currentTimeMillis() ;
        List<String> workers = waitForActiveWorkerChange(waitTime) ;
        if(workers == null) {
          workers = activeWorkersNode.getChildren();
        }
        if(workers.size() == 0) return true;
        long duration = System.currentTimeMillis() - start ;
        waitTime = waitTime - duration ;
      }
      return false ;
    }
  }
}
