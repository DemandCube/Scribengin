package com.neverwinterdp.scribengin.commitlog;

import java.util.Arrays;
import java.util.LinkedList;

public class InMemoryCommitLog implements CommitLog {

  private LinkedList<CommitLogEntry> commitLogList;
  
  public InMemoryCommitLog(){
    commitLogList = new LinkedList<CommitLogEntry>();
  }
  
  @Override
  public boolean addNextEntry(CommitLogEntry c) {
    return commitLogList.add(c);
  }

  @Override
  public CommitLogEntry[] getCommitLogs() {
    return Arrays.copyOf( this.commitLogList.toArray(),  this.commitLogList.toArray().length, CommitLogEntry[].class);
  }

}
