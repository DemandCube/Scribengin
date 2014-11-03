package com.neverwinterdp.scribengin.source.ri;

import java.util.List;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.source.CommitPoint;
import com.neverwinterdp.scribengin.source.DataSourceReader;

/**
 * @author Tuan Nguyen
 */
public class DataSourceReaderImpl implements DataSourceReader {
  private DataSourceImpl datasource ;
  private List<Record> records ;
  private int commitPoint ;
  private int currPosition ;
  
  public DataSourceReaderImpl(DataSourceImpl datasource, List<Record> records) {
    this.datasource = datasource ;
    this.records    = records ;
  }
  
  public Record next() throws Exception {
    if(currPosition < records.size()) return records.get(currPosition++) ;
    return null;
  }

  public void rollback() throws Exception {
    currPosition = commitPoint ;
  }
  
  public CommitPoint commit() throws Exception {
    CommitPoint cp = new CommitPoint(datasource.getName(), commitPoint, currPosition) ;
    this.commitPoint = currPosition ;
    return cp ;
  }
  
  public void close() throws Exception {
    
  }
}