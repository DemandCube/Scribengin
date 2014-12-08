package com.neverwinterdp.scribengin.sink;

public class SinkStreamDescriptor {
  private String type;
  private int    id;
  private String location;

  public SinkStreamDescriptor() { }
  
  public SinkStreamDescriptor(String type, int id, String location) {
    this.type = type;
    this.id = id;
    this.location = location;
  }
  
  public String getType() { return this.type; }
  public void setType(String type) {
    this.type = type ;
  }
  
  public int getId() { return id; }
  public void setId(int id) { this.id = id ; }

  public String getLocation() { return location; }
  public void   setLocation(String location) { this.location = location; }
}
