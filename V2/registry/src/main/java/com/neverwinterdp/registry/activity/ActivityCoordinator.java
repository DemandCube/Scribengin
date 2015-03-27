package com.neverwinterdp.registry.activity;

public interface ActivityCoordinator {
  public void onCreate(Activity activity);
  
  public void onAssign(Activity activity, ActivityStep step) ;
  public void onFinish(Activity activity, ActivityStep step) ;
  
  public void onFinishCreate(Activity activity);
}
