package com.neverwinterdp.scribengin.utils;

public class Progress {

  private int id;
  private int offset;
  private String timestamp;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "Progress [id=" + id + ", offset=" + offset + ", timestamp=" + timestamp + "]";
  }
}
