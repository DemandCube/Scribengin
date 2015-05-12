package com.neverwinterdp.registry;

import com.neverwinterdp.util.text.DateUtil;


public class RegistryLogger {
  private Node     logNode ;
  
  public RegistryLogger(Registry registry, String path) throws RegistryException {
    logNode = registry.createIfNotExist(path) ;
  }
  
  public void info(String name, String mesg) throws RegistryException {
    Log log = new Log("INFO", mesg) ;
    logNode.createChild(DateUtil.asCompactDate(log.getTimestamp()) + "-INFO-" + name + "-", log, NodeCreateMode.PERSISTENT_SEQUENTIAL);
  }
  
  public void error(String name, String mesg) throws RegistryException {
    Log log = new Log("ERROR", mesg) ;
    logNode.createChild(DateUtil.asCompactDate(log.getTimestamp()) +  "-ERROR-" + name + "-", log, NodeCreateMode.PERSISTENT_SEQUENTIAL);
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
  }
}

