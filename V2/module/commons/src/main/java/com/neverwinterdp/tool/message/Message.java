package com.neverwinterdp.tool.message;

public class Message {
  private int    partition ;
  private int    trackId ;
  private byte[] data ;
  
  public Message() { }
  
  public Message(int partition, int trackId, int messageSize) {
    this.partition = partition ;
    this.trackId = trackId ;
    this.data    = new byte[messageSize] ;
  }

  public int getPartition() { return partition; }
  public void setPartition(int partition) { this.partition = partition; }

  public int  getTrackId() { return trackId; }
  public void setTrackId(int trackId) { this.trackId = trackId; }

  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
}