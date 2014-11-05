package com.neverwinterdp.scribengin.sink.ri;

import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.DataStreamWriter;

public class DataStreamWriterImpl implements DataStreamWriter {
  private List<Record> dataHolder ;
  private boolean commit = false ;
  
  public DataStreamWriterImpl(List<Record> dataHolder) {
    this.dataHolder = dataHolder ;
  }
  
  @Override
  public void append(Record record) throws Exception {
    if(commit) throw new Exception("This stream is already commit and no longer modifiable") ;
    dataHolder.add(record) ;
  }

  @Override
  public void commit() throws Exception {
    commit = true ;
  }
  
  @Override
  public void close() throws Exception {
    if(!commit) commit() ;
  }
  
}
