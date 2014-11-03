package com.neverwinterdp.scribengin.dataflow.ri;

import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataStreamWriter;

public class DataStreamWriterImpl implements DataStreamWriter {
  private List<Record> dataHolder ;
  
  public DataStreamWriterImpl(List<Record> dataHolder) {
    this.dataHolder = dataHolder ;
  }
  
  @Override
  public void append(Record record) throws Exception {
    dataHolder.add(record) ;
  }

  @Override
  public void close() throws Exception {
  }
  
}
