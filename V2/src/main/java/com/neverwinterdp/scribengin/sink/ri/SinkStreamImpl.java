package com.neverwinterdp.scribengin.sink.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class SinkStreamImpl implements SinkStream {
  private int index ;
  private List<Record> dataHolder = new ArrayList<Record>() ;
  
  public SinkStreamImpl(int index) {
    this.index = index ;
  }
  
  @Override
  public int getId() { return index; }

  @Override
  public SinkStreamWriter getWriter() {
    return new SinkStreamWriterImpl(dataHolder);
  }
}
