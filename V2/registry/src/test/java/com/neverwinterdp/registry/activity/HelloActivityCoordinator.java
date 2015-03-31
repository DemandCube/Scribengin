package com.neverwinterdp.registry.activity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;

@Singleton
public class HelloActivityCoordinator extends ActivityCoordinator {
  final static public String ACTIVITIES_PATH = "/activities" ;
  
  @Inject
  private HelloActivityStepWorkerService workerService ;
  
  public void onResume(ActivityService service, Activity activity) {
    System.err.println("On resume activity: " + activity.getDescription()) ;
  }

  @Override
  public void onExecuting(ActivityService service, Activity activity, ActivityStep step)  {
    super.onExecuting(service, activity, step);
    System.err.println("On assign activity step: " + activity.getDescription()) ;
  }

  @Override
  public void onBroken(ActivityService service, Activity activity, ActivityStep step) throws RegistryException {
    super.onBroken(service, activity, step);
    System.err.println("On activity step broken: " + activity.getDescription()) ;
  }
  
  @Override
  public void onFinish(ActivityService service, Activity activity, ActivityStep step) throws RegistryException {
    super.onFinish(service, activity, step);
    System.err.println("On finish activity step: " + activity.getDescription()) ;
  }

  @Override
  protected <T> void execute(ActivityService service, Activity activity, ActivityStep step) {
    workerService.exectute(activity, step);
  }
}