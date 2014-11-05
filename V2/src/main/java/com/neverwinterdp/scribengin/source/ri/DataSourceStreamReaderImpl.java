package com.neverwinterdp.scribengin.source.ri;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.DataSourceStream;
import com.neverwinterdp.scribengin.source.DataSourceStreamReader;

/**
 * @author Tuan Nguyen
 */
public class DataSourceStreamReaderImpl implements DataSourceStreamReader {
  private String name ;
  private DataSourceStreamImpl dataStream ;
  private List<Record> records ;
  private int commitPoint ;
  private int currPosition ;
  
  public DataSourceStreamReaderImpl(String name, DataSourceStreamImpl dataStream, List<Record> records) {
    this.name = name ;
    this.dataStream = dataStream ;
    this.records    = records ;
  }
  
  public String getName() { return name ; }
  
  public DataSourceStream getDataStream() { return this.dataStream ; }
  
  public Record next() throws Exception {
    if(currPosition < records.size()) return records.get(currPosition++) ;
    return null;
  }
  
  public Record[] next(int size) throws Exception {
    List<Record> holder = new ArrayList<Record>() ;
    for(int i = 0; i < size; i++) {
      Record record = next() ;
      if(record != null) holder.add(record) ;
      else break ;
    }
    Record[] array = new Record[holder.size()] ;
    holder.toArray(array) ;
    return array ;
  }

  public void rollback() throws Exception {
    currPosition = commitPoint ;
  }
  
  public CommitPoint commit() throws Exception {
    CommitPoint cp = new CommitPoint(name, commitPoint, currPosition) ;
    this.commitPoint = currPosition ;
    return cp ;
  }
  
  public void close() throws Exception {
    
  }
}