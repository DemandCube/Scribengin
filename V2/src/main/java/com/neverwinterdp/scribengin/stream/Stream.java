package com.neverwinterdp.scribengin.stream;

public interface Stream {
  public boolean prepareCommit();
  public boolean commit();
  public boolean clearBuffer();
  public boolean completeCommit();
  public boolean rollBack();
  
}
