package com.neverwinterdp.scribengin.dataflow.ri;

import java.util.Iterator;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataStreamReader;

public class DataStreamReaderImpl implements DataStreamReader {
  private Iterator<Record> iterator ;
  
  public DataStreamReaderImpl(Iterator<Record> iterator) {
    this.iterator = iterator ;
  }
  
  @Override
  public Record next() throws Exception {
    if(iterator.hasNext()) return iterator.next() ;
    return null;
  }

  @Override
  public void close() throws Exception {
  }

}
