package com.neverwinterdp.registry.activity;

import java.util.List;

public class Activity {
  private String             description;
  private String             coordinator;
  private String             type ;
  private String             id ;
  
  private List<ActivityStep> activitySteps;
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public String getType() {  return type; }
  public void setType(String type) { this.type = type; }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getCoordinator() { return coordinator; }
  public void setCoordinator(String coordinator) { this.coordinator = coordinator; }
  
  public void withCoordinator(Class<?> type) {
    this.coordinator = type.getName();
  }
  
  public List<ActivityStep> getActivitySteps() { return activitySteps; }
  public void setActivitySteps(List<ActivityStep> activitySteps) { this.activitySteps = activitySteps; }
  
  public ActivityCoordinator newActivityCoordinator() throws Exception {
    Class<ActivityCoordinator> type = (Class<ActivityCoordinator>) Class.forName(coordinator) ;
    return type.newInstance();
  }
}
