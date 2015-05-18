package com.neverwinterdp.registry.notification;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class NotificationEvent {
  static public enum Level {INFO, WARNING, ERROR} ;
  
  private String seqId ;
  private long   timestamp ;
  private Level  level ;
  private String name ;
  private String message ;
  
  public NotificationEvent() {}
  
  public NotificationEvent(Level level, String name, String message) {
    this.timestamp = System.currentTimeMillis() ;
    this.level = level ;
    this.name  = name  ;
    this.message = message ;
  }

  @JsonIgnore
  public String getSeqId() { return this.seqId; }
  public void   setSeqId(String id) { this.seqId = id ; }
  
  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  public Level getLevel() { return level; }
  public void  setLevel(Level level) { this.level = level; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getMessage() { return message; }
  public void setMessage(String message) { this.message = message; }
  
  @JsonIgnore
  public String getTimestampId() {
    return Notifier.COMPACT_DATE_TIME_ID.format(new Date(timestamp)) ;
  }
}