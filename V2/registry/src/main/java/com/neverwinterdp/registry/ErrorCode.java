package com.neverwinterdp.registry;

public enum ErrorCode {
  Connection(1, "Cannot connect to the registry server due to the error such the unavailable server, timeout.."),
  Timeout(2, "Cannot wait for an event, or wait too long"),
  NodeCreation(3, "Cannot create node due to certain error such, no parent node..."),
  NodeAccess(4, "Cannot access the node due to certain error such permission"),
  NoNode(5, "No such node exists"),
  NodeExists(6, "A node is already existed"),
  Unknown(1000,  "Unclassified error");
  
  private int code;
  private String description ;

  ErrorCode(int code, String description) {
      this.code = code;
      this.description = description ;
  }
  
  public int getCode() { return this.code ; }
  
  public String getDescription() { return this.description ; }
}
