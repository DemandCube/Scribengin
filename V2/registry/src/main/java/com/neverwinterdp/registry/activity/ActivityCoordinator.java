package com.neverwinterdp.registry.activity;

public interface ActivityCoordinator {
  public void onStart(Activity activity);
  
  public void onResume(Activity activity) ;
  
  public void onAssign(Activity activity, ActivityStep step) ;
  public void onFinish(Activity activity, ActivityStep step) ;
  
  public void onFinish(Activity activity);
}
