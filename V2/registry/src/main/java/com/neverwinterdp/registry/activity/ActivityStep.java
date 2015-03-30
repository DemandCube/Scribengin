package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;

public class ActivityStep {
  static public enum Status { INIT, ASSIGNED, FINISHED }
  
  private String             description;
  private String             type ;
  private String             id ;
  private Status             status = Status.INIT;
  private String             executor;
  private int                maxRetries = 1;
  private List<String>       logs ;
  private ActivityStepResult result;
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public ActivityStep withDescription(String desc) {
    this.description = desc;
    return this;
  }
  
  public String getType() { return type; }
  public void   setType(String name) { this.type = name; }
  
  public ActivityStep withType(String name) {
    this.type = name;
    return this;
  }
  
  public String getId() { return id; }
  public void setId(String id) { this.id = id; }
  
  public Status getStatus() { return status; }
  public void setStatus(Status status) { this.status = status; }
  
  public String getExecutor() { return executor; }
  public void setExecutor(String executor) { this.executor = executor; }
  
  public ActivityStep withExecutor(Class<?> type) {
    this.executor = type.getName();
    return this;
  }
  
  public int getMaxRetries() { return maxRetries; }
  public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
  
  public List<String> getLogs() { return logs; }
  public void setLogs(List<String> logs) { this.logs = logs;}

  public void addLog(String log) {
    if(logs == null) logs = new ArrayList<String>();
    logs.add(log) ;
  }
  
  public ActivityStepResult getResult() { return result; }
  public void setResult(ActivityStepResult result) { this.result = result; }
}