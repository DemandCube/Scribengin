package com.neverwinterdp.scribengin.storage.source;

import com.neverwinterdp.scribengin.Record;

/**
 * @author Tuan Nguyen
 */
public interface SourceStreamReader {
  public String getName() ;
  public Record next() throws Exception;
  public Record[] next(int size) throws Exception ;
  public void rollback() throws Exception;
  public void prepareCommit() throws Exception ;
  public void completeCommit() throws Exception ;
  public void commit() throws Exception;
  public CommitPoint getLastCommitInfo() ;
  public void close() throws Exception;
}
