package com.neverwinterdp.scribengin.source;

import com.neverwinterdp.scribengin.Record;

/**
 * @author Tuan Nguyen
 */
public interface DataSourceReader {
  public Record next() throws Exception;

  public void rollback() throws Exception;

  public CommitPoint commit() throws Exception;

  public void close() throws Exception;
}
