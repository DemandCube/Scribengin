package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;

abstract public class ActivityBuilder {
  private Activity activity = new Activity();
  private List<ActivityStep> activitySteps = new ArrayList<>();
  
  @Deprecated
  public Activity getActivity() { return activity ; }
  
  @Deprecated
  public List<ActivityStep> getActivitySteps() { return this.activitySteps ; }
  
  @Deprecated
  protected void add(ActivityStep step) { activitySteps.add(step) ; }
}
