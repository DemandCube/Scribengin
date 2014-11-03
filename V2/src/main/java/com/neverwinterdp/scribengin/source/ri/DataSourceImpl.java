package com.neverwinterdp.scribengin.source.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.DataSource;
import com.neverwinterdp.scribengin.source.DataSourceReader;

/**
 * @author Tuan Nguyen
 */
public class DataSourceImpl implements DataSource {
  private String name ;
  private List<Record> records ;
  
  public DataSourceImpl(String name, List<Record> records) {
    this.name = name ;
    this.records = records ;
  }
  
  public DataSourceImpl(String name, int size) {
    this(name, generate(size, 32)) ;
  }
  
  public String getName() { return name; }

  @Override
  public DataSourceReader getReader() {
    return new DataSourceReaderImpl(this, new ArrayList<Record>(records));
  }

  static public List<Record> generate(int size, int dataSize) {
    List<Record> records = new ArrayList<Record>() ;
    for(int i = 0; i < size; i++) {
      records.add(new Record()) ;
    }
    return records ;
  }
}
