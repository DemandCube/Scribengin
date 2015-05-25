package com.neverwinterdp.registry;

public class RefNode {
  private String path ;

  public RefNode() {}
  
  public RefNode(String path) {
    this.path = path;
  }
  
  public String getPath() { return path; }
  public void setPath(String path) { this.path = path; }
}
