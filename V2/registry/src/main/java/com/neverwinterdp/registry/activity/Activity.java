package com.neverwinterdp.registry.activity;

import java.util.List;

public class Activity {
  private String description ;
  private String registryPath;
  private String coordinator ;
  private List<ActivityStep> activitySteps ;
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public String getCoordinator() { return coordinator; }
  public void setCoordinator(String coordinator) { this.coordinator = coordinator; }
  
  public List<ActivityStep> getActivitySteps() { return activitySteps; }
  public void setActivitySteps(List<ActivityStep> activitySteps) { this.activitySteps = activitySteps; }
}
