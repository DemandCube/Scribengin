package com.neverwinterdp.scribengin.stream.streamdescriptor;

public class OffsetStreamDescriptor implements StreamDescriptor{
  private int currentOffset;
  private int lastCommittedOffset;
  private String name;
  
  public OffsetStreamDescriptor(String name){
    this(name, 0,0);
  }
  
  public OffsetStreamDescriptor(String name, int lastCommittedOffset, int currentOffset){
    this.name = name;
    this.setLastCommittedOffset(lastCommittedOffset);
    this.setCurrentOffset(currentOffset);
  }

  public int getCurrentOffset() {
    return currentOffset;
  }

  public int getLastCommittedOffset() {
    return lastCommittedOffset;
  }

  public void setCurrentOffset(int currentOffset) {
    this.currentOffset = currentOffset;
  }

  public void setLastCommittedOffset(int lastCommittedOffset) {
    this.lastCommittedOffset = lastCommittedOffset;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }
}
