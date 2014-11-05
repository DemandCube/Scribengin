package com.neverwinterdp.scribengin.source;

import com.neverwinterdp.scribengin.Record;

/**
 * @author Tuan Nguyen
 */
public interface DataSourceStreamReader {
  public String getName() ;
  
  public DataSourceStream getDataStream() ;
  
  public Record next() throws Exception;
  
  public Record[] next(int size) throws Exception ;

  public void rollback() throws Exception;

  public CommitPoint commit() throws Exception;

  public void close() throws Exception;
}
