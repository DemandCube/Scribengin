package com.neverwinterdp.registry;

public class NodeEvent {
  static public enum Type { CREATE, DELETE, MODIFY, CHILDREN_CHANGED, LOCK, UNLOCK, OTHER}
  
  private String path ;
  private Type   type ;

  public NodeEvent() {}
  
  public NodeEvent(String path, Type type) {
    this.path = path ;
    this.type = type ;
  }
  
  public String getPath() { return path; }
  public void setPath(String path) { this.path = path; }
  
  public Type getType() { return type; }
  public void setType(Type type) { this.type = type; }
}
