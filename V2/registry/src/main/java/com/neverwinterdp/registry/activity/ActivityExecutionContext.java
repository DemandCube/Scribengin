package com.neverwinterdp.registry.activity;

public class ActivityExecutionContext {
  private Activity            activity;
  private ActivityService     activityService;
  private ActivityCoordinator activityCoordinator;
  private boolean             abort = false;

  public ActivityExecutionContext(Activity activity, ActivityService service) {
    this.activity = activity;
    this.activityService = service ;
  }
  
  public Activity getActivity() { return activity; }
  
  public ActivityService getActivityService() { return activityService; }

  public ActivityCoordinator getActivityCoordinator() { return activityCoordinator; }
  public void setActivityCoordinator(ActivityCoordinator coordinator) {
    this.activityCoordinator = coordinator;
  }

  public boolean isAbort() { return abort; }
  public void setAbort(boolean abort) {
    this.abort = abort;
  }

  public synchronized void notifyTermination() {
    notifyAll();
  }
  
  public synchronized void waitForTermination(long timeout) throws InterruptedException {
    wait(timeout);
  }
}
