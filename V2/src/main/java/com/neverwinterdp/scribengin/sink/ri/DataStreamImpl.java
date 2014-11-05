package com.neverwinterdp.scribengin.sink.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.DataStream;
import com.neverwinterdp.scribengin.sink.DataStreamWriter;

public class DataStreamImpl implements DataStream {
  private int index ;
  private List<Record> dataHolder = new ArrayList<Record>() ;
  
  public DataStreamImpl(int index) {
    this.index = index ;
  }
  
  @Override
  public int getIndex() { return index; }

  @Override
  public DataStreamWriter getWriter() {
    return new DataStreamWriterImpl(dataHolder);
  }
}
