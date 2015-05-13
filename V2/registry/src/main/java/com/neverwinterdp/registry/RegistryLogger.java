package com.neverwinterdp.registry;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class RegistryLogger {
  final static public SimpleDateFormat COMPACT_DATE_TIME_ID = new SimpleDateFormat("dd-MM-yyyy@HH:mm:ss") ;
  
  private Node     logNode ;
  
  public RegistryLogger(Registry registry, String path) throws RegistryException {
    logNode = registry.createIfNotExist("/logs/" + path + "-logger") ;
  }
  
  public void info(String name, String mesg) throws RegistryException {
    Log log = new Log("INFO", mesg) ;
    logNode.createChild(log.getTimestampId() + "-INFO-" + name + "-", log, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public void error(String name, String mesg) throws RegistryException {
    Log log = new Log("ERROR", mesg) ;
    logNode.createChild(log.getTimestampId() +  "-ERROR-" + name + "-", log, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  static public class Log {
    private long   timestamp ;
    private String level ;
    private String message ;
    
    public Log() {}
    
    public Log(String level, String message) {
      this.timestamp = System.currentTimeMillis() ;
      this.level = level ;
      this.message = message ;
    }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    
    @JsonIgnore
    public String getTimestampId() {
      return COMPACT_DATE_TIME_ID.format(new Date(timestamp)) ;
    }
  }
}

