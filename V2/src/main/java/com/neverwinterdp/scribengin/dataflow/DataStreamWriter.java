package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;

public interface DataStreamWriter {
  public void append(Record record) throws Exception ;
  public void close() throws Exception ;
}
