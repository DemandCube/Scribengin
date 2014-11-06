package com.neverwinterdp.scribengin.source;

public interface DataSourceStream {
  public int getId();
  
  public String getLocation() ;
  
  public DataSourceStreamReader getReader(String name);
}
