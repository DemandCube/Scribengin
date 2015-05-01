package com.neverwinterdp.registry.activity;

import com.neverwinterdp.registry.RegistryException;

public class ActivityCoordinatorAdapter {
  public void beforeOnStart(ActivityExecutionContext ctx, Activity activity) throws Exception {
  }
  
  public void afterOnStart(ActivityExecutionContext ctx, Activity activity) throws Exception {
  }
  
  public void beforeOnResume(ActivityExecutionContext ctx, Activity activity) throws Exception {
  }

  public void afterOnResume(ActivityExecutionContext ctx, Activity activity) throws Exception {
  }

  
  public void beforeOnFinish(ActivityExecutionContext ctx, Activity activity) throws RegistryException {
  }
  
  public void afterOnFinish(ActivityExecutionContext ctx, Activity activity) throws RegistryException {
  }
  
  public void beforeExecute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
    
  }
  
  public void afterExecute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
    
  }
}
