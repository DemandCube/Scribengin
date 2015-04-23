package com.neverwinterdp.registry.activity;

public class ActivityExecutionContext {
  private Activity activity ;
  private ActivityService activityService ;
  private ActivityCoordinator activityCoordinator;
  
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
  
  public synchronized void notifyTermination() {
    notifyAll();
  }
  
  public synchronized void waitForTermination(long timeout) throws InterruptedException {
    wait(timeout);
  }
}
