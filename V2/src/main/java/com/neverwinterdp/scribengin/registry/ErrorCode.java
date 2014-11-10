package com.neverwinterdp.scribengin.registry;

public enum ErrorCode {
  Connection(1, "Cannot connect to the registry server due to the error such the unavailable server, timeout.."),
  NodeCreation(2, "Cannot create node due to certain error such, connection, timeout..."),
  NodeAccess(3, "Cannot access the node due to certain error such permission"),
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
