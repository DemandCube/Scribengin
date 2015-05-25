package com.neverwinterdp.registry;

public interface NodeInfo {
  public long getCtime() ;

  public long getMtime() ;

  public int getVersion() ;

  public int getDataLength() ;

  public int getNumOfChildren() ;
}
  