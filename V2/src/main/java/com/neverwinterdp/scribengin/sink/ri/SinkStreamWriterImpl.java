package com.neverwinterdp.scribengin.sink.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class SinkStreamWriterImpl implements SinkStreamWriter {
  private List<Record> dataHolder ;
  private List<Record> tmpHolder  ;
  
  public SinkStreamWriterImpl(List<Record> dataHolder) {
    this.dataHolder = dataHolder ;
    this.tmpHolder = new ArrayList<Record>() ;
  }
  
  @Override
  synchronized public void append(Record record) throws Exception {
    tmpHolder.add(record) ;
  }

  @Override
  synchronized public void commit() throws Exception {
    dataHolder.addAll(tmpHolder) ;
    tmpHolder.clear(); 
  }
  
  @Override
  synchronized public void close() throws Exception {
    if(tmpHolder.size() > 0) commit() ;
  }

  @Override
  public boolean verifyLastCommit() throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean discard() throws Exception {
    // TODO Auto-generated method stub
    return false;
  }
  
}
