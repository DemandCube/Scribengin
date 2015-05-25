package com.neverwinterdp.scribengin.dataflow.test;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class ExecuteLog {
  private long         start;
  private long         stop;
  private String       description;
  private boolean      success = true;
  private List<String> logs = new ArrayList<String>();

  public ExecuteLog() {} 
  
  public ExecuteLog(String description) {
    this.description = description ;
  }
   
  public long getStart() { return start; }
  public void setStart(long start) { this.start = start; }
  
  public long getStop() { return stop; }
  public void setStop(long stop) { this.stop = stop; }
  
  public String getDescription() { return description; }
  public void   setDescription(String description) { this.description = description; }
  
  public boolean isSuccess() { return success; }
  public void setSuccess(boolean success) { this.success = success; }
  
  public List<String> getLogs() { return logs; }
  public void setLogs(List<String> logs) { this.logs = logs; }
  
  public void addLog(String message) {
    logs.add(message) ;
  }
  
  public void addLog(Object ... word) {
    String message = StringUtil.join(word, " ");
    logs.add(message) ;
  }
  
  public void start() {
    this.start = System.currentTimeMillis() ;
  }
  
  public void stop() {
    this.stop = System.currentTimeMillis() ;
  }

  public String getFormatText() {
    StringBuilder b = new StringBuilder() ;
    TabularFormater formater = new TabularFormater("Property", "Value") ;
    formater.setTitle(description);
    formater.addRow("Duration", (stop - start) + "ms");
    formater.addRow("Success", success);
    b.append(formater.getFormatText()).append("\n");
    b.append("Message Logs:\n");
    for(int i = 0; i < logs.size(); i++) {
      b.append("* message " + (i + 1)).append("\n");
      b.append(logs.get(i)).append("\n");
    }
    return b.toString();
  }
}