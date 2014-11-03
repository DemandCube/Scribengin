package com.neverwinterdp.scribengin.dataflow.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataStream;
import com.neverwinterdp.scribengin.dataflow.DataStreamReader;
import com.neverwinterdp.scribengin.dataflow.DataStreamWriter;

public class DataStreamImpl implements DataStream {
  private int index ;
  private List<Record> dataHolder = new ArrayList<Record>() ;
  
  public DataStreamImpl(int index) {
    this.index = index ;
  }
  
  @Override
  public int getIndex() { return index; }

  @Override
  public DataStreamReader getReader() {
    return new DataStreamReaderImpl(dataHolder.iterator());
  }

  @Override
  public DataStreamWriter getWriter() {
    return new DataStreamWriterImpl(dataHolder);
  }
}
