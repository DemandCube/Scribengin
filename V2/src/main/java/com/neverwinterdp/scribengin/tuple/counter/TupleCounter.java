package com.neverwinterdp.scribengin.tuple.counter;

public interface TupleCounter {
  public void incrementValid();
  public void incrementWritten();
  //public void incrementProcessed();
  public void incrementInvalid();
  public void incrementCreated();
  
  public void addValid(long toAdd);
  public void addWritten(long toAdd);
  //public void addProcessed(int toAdd);
  public void addInvalid(long toAdd);
  public void addCreated(long toAdd);
  
  public long getValid();
  public long getWritten();
  //public long getProcessed();
  public long getInvalid();
  public long getCreated();
  
  public boolean validateCounts();
  
  public void commit();
  public void clearBuffer();
  
  @Override
  public String toString();
}
