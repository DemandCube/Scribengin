package com.neverwinterdp.registry.activity;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class HelloActivityCoordinator extends ActivityCoordinator {
  final static public String ACTIVITIES_PATH = "/activities" ;
  
  @Inject
  private HelloActivityStepWorkerService workerService ;
  
  public void onResume(ActivityService service, Activity activity) {
    System.err.println("On resume activity: " + activity.getDescription()) ;
  }

  @Override
  protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
    workerService.exectute(context, activity, step);
  }
}