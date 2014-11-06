package com.neverwinterdp.scribengin.source;
/**
 * @author Tuan Nguyen
 */
public interface DataSource {
  public String getName() ;
  public String getLocation() ;
  public DataSourceStream   getDataStream(int id) ;
  public DataSourceStream[] getDataStreams() ;
}