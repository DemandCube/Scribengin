package com.neverwinterdp.scribengin.stream;

public class StreamDescriptor {
  static public enum Status { INIT, PROCESSING, SUSPENDED, TERMINATED }
  private int id ;
  private Status status = Status.INIT ;
  
  public int getId() { return id ; }
  public void setId(int id) { this.id = id ; }
  
  public Status getStatus() { return this.status ; }
  public void setStatus(Status status) {
    this.status = status ;
  }
}
