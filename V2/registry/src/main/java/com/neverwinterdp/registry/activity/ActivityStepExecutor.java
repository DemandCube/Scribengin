package com.neverwinterdp.registry.activity;

public interface ActivityStepExecutor {
  public void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception ;
}
