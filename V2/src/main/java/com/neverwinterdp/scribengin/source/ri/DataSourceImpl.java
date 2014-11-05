package com.neverwinterdp.scribengin.source.ri;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.DataSource;
import com.neverwinterdp.scribengin.source.DataSourceStream;

/**
 * @author Tuan Nguyen
 */
public class DataSourceImpl implements DataSource {
  private String name ;
  private Map<Integer,DataSourceStreamImpl> streams = new LinkedHashMap<Integer, DataSourceStreamImpl>();
  
  public DataSourceImpl(String name) {
    this.name = name ;
  }
  
  public DataSourceImpl(String name, int numOfStream, int streamSize) {
    this(name) ;
    for(int i = 0; i < numOfStream; i++) {
      DataSourceStreamImpl stream = new DataSourceStreamImpl(i, generate(streamSize, 32)) ;
      streams.put(i, stream) ;
    }
  }
  
  public String getName() { return name; }

  public String getLocation() { return "inmemory" ; }
  
  public DataSourceStream   getDataStream(int id) { return streams.get(id) ; }
  
  public DataSourceStream[] getDataStreams() {
    DataSourceStream[] array = new DataSourceStream[streams.size()];
    return streams.values().toArray(array);
  }
  
  
  
  static public List<Record> generate(int size, int dataSize) {
    List<Record> records = new ArrayList<Record>() ;
    for(int i = 0; i < size; i++) {
      records.add(new Record()) ;
    }
    return records ;
  }
}
