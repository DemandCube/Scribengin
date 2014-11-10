package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.Record;

public interface SinkStreamWriter {
  public void append(Record record) throws Exception ;
  public void commit() throws Exception ;
  public void close()  throws  Exception ;
  public boolean verifyLastCommit() throws Exception;
  
  //Used to rollback or discard a commit that has failed
  public boolean discard() throws Exception;
}
