package com.neverwinterdp.registry.activity;

public interface ActivityStepExecutor {
  public <T> T getWorkerInfo() ;
  public void execute(Activity activity, ActivityStep step) ;
}
