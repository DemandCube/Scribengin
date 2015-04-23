package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Activity {
  private String              description;
  private String              type;
  private String              id;
  private String              coordinator;
  private String              activityStepBuilder;
  private Map<String, String> attributes = new HashMap<>();
  private List<String>        logs;

  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public String getType() {  return type; }
  public void setType(String type) { this.type = type; }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public String getCoordinator() { return coordinator; }
  public void setCoordinator(String coordinator) { this.coordinator = coordinator; }
  
  public Activity withCoordinator(Class<?> type) {
    this.coordinator = type.getName();
    return this;
  }
  
  public String getActivityStepBuilder() { return activityStepBuilder; }
  public void setActivityStepBuilder(String activityStepBuilder) {
    this.activityStepBuilder = activityStepBuilder;
  }
  
  public Activity withActivityStepBuilder(Class<?> type) {
    this.activityStepBuilder = type.getName();
    return this;
  }
  
  public Map<String, String> getAttributes() { return attributes; }
  public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }
  
  public void attribute(String name, String value) { attributes.put(name, value); }
  
  public void attribute(String name, int value) { attributes.put(name, Integer.toString(value)); }
  
  public void attribute(String name, boolean value) { attributes.put(name, Boolean.toString(value)); }
  
  public String attribute(String name) { return attributes.get(name); }
  
  public int attributeAsInt(String name, int defaultValue) { 
    String val = attributes.get(name); 
    return val != null ? Integer.parseInt(val) : defaultValue;
  }
  
  public boolean attributeAsBoolean(String name, boolean defaultValue) { 
    String val = attributes.get(name); 
    return val != null ? Boolean.parseBoolean(val) : defaultValue;
  }
  
  public List<String> getLogs() { return logs; }
  public void setLogs(List<String> logs) { this.logs = logs; }

  public void addLog(String log) {
    if(logs == null) logs = new ArrayList<String>();
    logs.add(log) ;
  }
  
}