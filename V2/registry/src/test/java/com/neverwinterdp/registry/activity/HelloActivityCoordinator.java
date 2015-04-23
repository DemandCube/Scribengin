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
  public void onExecuting(ActivityExecutionContext context, Activity activity, ActivityStep step)  {
    super.onExecuting(context, activity, step);
    System.err.println("On assign activity step: " + activity.getDescription()) ;
  }

  @Override
  public void onBroken(ActivityExecutionContext context, Activity activity, ActivityStep step) throws RegistryException {
    super.onBroken(context, activity, step);
    System.err.println("On activity step broken: " + activity.getDescription()) ;
  }
  
  @Override
  public void onFinish(ActivityExecutionContext context, Activity activity, ActivityStep step) throws RegistryException {
    super.onFinish(context, activity, step);
    System.err.println("On finish activity step: " + activity.getDescription()) ;
  }

  @Override
  protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) {
    workerService.exectute(context, activity, step);
  }
}