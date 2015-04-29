package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

abstract public class ActivityCoordinator {
  public void onStart(ActivityExecutionContext ctx, Activity activity) throws Exception {
    ActivityService service = ctx.getActivityService();
    List<ActivityStep> activitySteps = service.getActivitySteps(activity);
    execute(ctx, activity, activitySteps);
  }
  
  public void onResume(ActivityExecutionContext ctx, Activity activity) throws Exception {
  }
  
  public void onFinish(ActivityExecutionContext ctx, Activity activity) throws RegistryException {
    ActivityService service = ctx.getActivityService();
    service.history(activity);
    synchronized(this) {
      notifyAll();
    }
  }
  
  void execute(ActivityExecutionContext ctx, Activity activity, List<ActivityStep> activitySteps) throws Exception {
    for(int i = 0; i < activitySteps.size(); i++) {
      ActivityStep nextStep = activitySteps.get(i);
      nextStep.setStatus(ActivityStep.Status.ASSIGNED);
      schedule(ctx, activity, nextStep);
      if(ctx.isAbort()) break;
      nextStep.setStatus(ActivityStep.Status.FINISHED);
    }
    onFinish(ctx, activity);
  }
  
  
  synchronized public void waitForTermination(long timeout) throws InterruptedException {
    wait(timeout);
  }
  
  public void shutdown() {
  }
  
  protected List<ActivityStep> findNextActivitySteps(ActivityService service, Activity activity) throws RegistryException {
    List<ActivityStep> nextStepHolder = new ArrayList<>() ;
    List<ActivityStep> activitySteps = service.getActivitySteps(activity);
    for(int i = 0; i < activitySteps.size(); i++) {
      ActivityStep step = activitySteps.get(i);
      if(ActivityStep.Status.INIT.equals(step.getStatus())) {
        nextStepHolder.add(step) ;
        break;
      }
    }
    return nextStepHolder;
  }
  
  protected <T> void schedule(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
    ActivityService service = ctx.getActivityService();
    service.updateActivityStepAssigned(activity, step);
    execute(ctx, activity, step);
  }
  
  abstract protected <T> void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception ;
}