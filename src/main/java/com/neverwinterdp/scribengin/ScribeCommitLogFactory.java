package com.neverwinterdp.scribengin;

import java.io.IOException;

public class ScribeCommitLogFactory extends AbstractScribeCommitLogFactory {
  private static ScribeCommitLogFactory inst = null;
  private String commitLogPath;

  private ScribeCommitLogFactory(String commitLogPath)
  {
    this.commitLogPath = commitLogPath;
  }

  public static ScribeCommitLogFactory instance(String commitLogPath)
  {
    if (inst == null)
      inst = new ScribeCommitLogFactory(commitLogPath);
    return inst;

  }


  public ScribeCommitLog build() throws IOException
  {
    return new ScribeCommitLog(commitLogPath);
  }

}
