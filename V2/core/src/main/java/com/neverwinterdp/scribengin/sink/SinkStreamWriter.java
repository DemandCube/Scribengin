package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.Record;

public interface SinkStreamWriter {
  /**
   * @param record
   * @return true if we should keep appending, false if ready to commit
   * @throws Exception
   */
  public void append(Record record) throws Exception ;
  public void commit() throws Exception ;
  public void close()  throws  Exception ;
  public void rollback() throws Exception;
  public void prepareCommit() throws Exception ;
  public void completeCommit() throws Exception ;
}
