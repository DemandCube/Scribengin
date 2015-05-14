package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

abstract public class ActivityCoordinator {
  private List<ActivityCoordinatorAdapter> adapters = new ArrayList<>();
  
  public void add(ActivityCoordinatorAdapter adapter) {
    adapters.add(adapter);
  }
  
  public void onStart(ActivityExecutionContext ctx, Activity activity) throws Exception {
    for(ActivityCoordinatorAdapter sel : adapters) sel.beforeOnStart(ctx, activity);
    ActivityService service = ctx.getActivityService();
    List<ActivityStep> activitySteps = service.getActivitySteps(activity);
    execute(ctx, activity, activitySteps);
    for(ActivityCoordinatorAdapter sel : adapters) sel.afterOnStart(ctx, activity);
  }
  
  public void onResume(ActivityExecutionContext ctx, Activity activity) throws Exception {
    for(ActivityCoordinatorAdapter sel : adapters) sel.beforeOnResume(ctx, activity);
    ActivityService service = ctx.getActivityService();
    List<ActivityStep> activitySteps = findUnfinishedActivitySteps(service, activity);
    execute(ctx, activity, activitySteps);
    for(ActivityCoordinatorAdapter sel : adapters) sel.afterOnResume(ctx, activity);
  }
  
  public void onFinish(ActivityExecutionContext ctx, Activity activity) throws RegistryException {
    for(ActivityCoordinatorAdapter sel : adapters) sel.beforeOnFinish(ctx, activity);
    ActivityService service = ctx.getActivityService();
    service.history(activity);
    synchronized(this) {
      notifyAll();
    }
    for(ActivityCoordinatorAdapter sel : adapters) sel.afterOnFinish(ctx, activity);
  }
  
  void execute(ActivityExecutionContext ctx, Activity activity, List<ActivityStep> activitySteps) throws Exception {
    ActivityService service = ctx.getActivityService();
    for(int i = 0; i < activitySteps.size(); i++) {
      long startTime = System.currentTimeMillis();
      ActivityStep nextStep = activitySteps.get(i);
      service.updateActivityStepAssigned(activity, nextStep);
      for(ActivityCoordinatorAdapter sel : adapters) sel.beforeExecute(ctx, activity, nextStep);
      execute(ctx, activity, nextStep);
      for(ActivityCoordinatorAdapter sel : adapters) sel.afterExecute(ctx, activity, nextStep);
      service.updateActivityStepFinished(activity, nextStep);
      if(ctx.isAbort()) break;
      long executeTime = System.currentTimeMillis() - startTime ;
      nextStep.setExecuteTime(executeTime);
      nextStep.setTryCount(i + 1);
    }
    onFinish(ctx, activity);
  }
  
  
  synchronized public void waitForTermination(long timeout) throws InterruptedException {
    wait(timeout);
  }
  
  public void shutdown() {
  }
  
  protected List<ActivityStep> findUnfinishedActivitySteps(ActivityService service, Activity activity) throws RegistryException {
    List<ActivityStep> nextStepHolder = new ArrayList<>() ;
    List<ActivityStep> activitySteps = service.getActivitySteps(activity);
    for(int i = 0; i < activitySteps.size(); i++) {
      ActivityStep step = activitySteps.get(i);
      if(!ActivityStep.Status.FINISHED.equals(step.getStatus())) {
        nextStepHolder.add(step) ;
        break;
      }
    }
    return nextStepHolder;
  }
  
  abstract protected <T> void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception ;
}