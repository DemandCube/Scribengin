package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.Record;

public interface SinkStreamWriter {
  /**
   * 
   * @param record
   * @return true if we should keep appending, false if ready to commit
   * @throws Exception
   */
  public boolean append(Record record) throws Exception ;
  public boolean commit() throws Exception ;
  public void close()  throws  Exception ;
  public boolean rollback() throws Exception;
  public boolean prepareCommit();
  public void completeCommit();
}
