package com.neverwinterdp.scribengin.dataflow.ri;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataStream;
import com.neverwinterdp.scribengin.dataflow.DataFlowReader;
import com.neverwinterdp.scribengin.dataflow.DataStreamReader;

public class DataFlowReaderImpl implements DataFlowReader {
  private DataStream[] dataStream ;
  private int          currStream = 0;
  private DataStreamReader currReader ;
  
  public DataFlowReaderImpl(DataStream[] dataStream) {
    this.dataStream = dataStream ;
  }
  
  @Override
  public Record next() throws Exception {
    if(currStream >= dataStream.length) return null ;
    
    if(currReader == null) {
      currReader = dataStream[currStream].getReader() ;
    }
    Record record = currReader.next() ;
    if(record != null) return record ;
    currReader.close();
    currReader = null ;
    currStream++ ;
    return next() ;
  }

  @Override
  public void close() throws Exception {
    if(currReader != null) currReader.close();
  }

}
