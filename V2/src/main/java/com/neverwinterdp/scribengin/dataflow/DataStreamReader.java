package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;

public interface DataStreamReader {
  public Record next() throws Exception ;
  public void close()  throws  Exception ;
}
