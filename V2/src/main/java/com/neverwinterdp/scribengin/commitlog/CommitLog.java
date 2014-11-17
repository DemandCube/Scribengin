package com.neverwinterdp.scribengin.commitlog;

public interface CommitLog {
  public boolean addNextEntry(CommitLogEntry c);
  public CommitLogEntry[] getCommitLogs();
}
