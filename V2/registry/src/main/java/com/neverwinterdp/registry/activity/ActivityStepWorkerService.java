package com.neverwinterdp.registry.activity;

import com.neverwinterdp.registry.RegistryException;

public class ActivityStepWorkerService<T> {
  private T workerDescriptor ;
  
  public ActivityStepWorkerService() {
  }
  
  public ActivityStepWorkerService(T workerDescriptor) throws RegistryException {
    init(workerDescriptor);
  }

  public void init(T workerDescriptor) throws RegistryException {
    this.workerDescriptor = workerDescriptor;
  }
  
  public T getWorkerDescriptor() { return workerDescriptor; }
  
  public void exectute(ActivityExecutionContext context, Activity activity, ActivityStep activityStep) throws Exception, InterruptedException {
    ActivityService service = context.getActivityService();
    service.updateActivityStepExecuting(activity, activityStep, getWorkerDescriptor());
    Exception error = null ;
    for(int i = 0; i < activityStep.getMaxRetries(); i++) {
      error = null;
      try {
        ActivityStepExecutor executor = service.getActivityStepExecutor(activityStep.getExecutor());
        executor.execute(context, activity, activityStep);
        return;
      } catch (Exception e) {
        activityStep.addLog("Fail to execute the activity due to the error: " + e.getMessage());
        context.setAbort(true);
        e.printStackTrace();
        error = e ;
      }
    }
    throw error ;
  }
}
