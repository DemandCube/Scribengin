package com.neverwinterdp.scribengin.source;
/**
 * @author Tuan Nguyen
 * 
 * 
 */
public interface DataSource {
  public String getName() ;
  public DataSourceReader getReader() ;
}