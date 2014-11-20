package com.neverwinterdp.scribengin.stream;

public interface Stream {
  public boolean prepareCommit();
  public boolean commit();
  public boolean clearCommit();
  public boolean updateOffSet();
  
}
