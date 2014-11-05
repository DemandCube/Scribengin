package com.neverwinterdp.scribengin.source.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.DataSourceStream;
import com.neverwinterdp.scribengin.source.DataSourceStreamReader;

public class DataSourceStreamImpl implements DataSourceStream {
  private int id ;
  private List<Record> records ;
  
  public DataSourceStreamImpl(int index) {
    this.id = index ;
    this.records = new ArrayList<Record>() ;
  }
  
  public DataSourceStreamImpl(int index, List<Record> records) {
    this.id = index ;
    this.records = records ;
  }
  
  @Override
  public int getId() { return this.id ; }

  public String getLocation() { return "inmemory:" + id ; }
  
  @Override
  public DataSourceStreamReader getReader(String name) {
    return new DataSourceStreamReaderImpl(name, this, records) ;
  }

}
