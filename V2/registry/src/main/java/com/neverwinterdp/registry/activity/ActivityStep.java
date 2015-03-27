package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;

public class ActivityStep {
  static public enum Status { INIT, ASSIGNED, FINISHED }
  
  private String             description;
  private Status             status = Status.INIT;
  private String             executor;
  private int                maxRetries = 1;
  private List<String>       logs ;
  private ActivityStepResult result;
  
  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }
  
  public Status getStatus() { return status; }
  public void setStatus(Status status) { this.status = status; }
  
  public String getExecutor() { return executor; }
  public void setExecutor(String executor) { this.executor = executor; }
  
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