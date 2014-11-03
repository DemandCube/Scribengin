package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.Record;

public interface SinkWriter {
  public void write(Record record) throws Exception ;
  public void commit() throws Exception ;
  public void close() throws Exception ;
}
