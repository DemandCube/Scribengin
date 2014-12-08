package com.neverwinterdp.scribengin.sink;

public class SinkStreamDescriptor {
  private String location;
  private int    id;

  public int getId() { return id; }
  public void setId(int id) { this.id = id ; }

  public String getLocation() { return location; }
  public void   setLocation(String location) { this.location = location; }
}
