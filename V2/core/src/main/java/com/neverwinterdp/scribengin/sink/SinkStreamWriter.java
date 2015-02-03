package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.Record;

public interface SinkStreamWriter {
  public void append(Record record) throws Exception ;
  public void commit() throws Exception ;
  public void close()  throws  Exception ;
  public boolean rollback() throws Exception;
}
