package com.neverwinterdp.scribengin.stream;

import java.io.IOException;

public interface Stream {
  public boolean prepareCommit();
  public boolean commit() throws IOException;
  public boolean clearBuffer();
  public boolean completeCommit();
  public boolean rollBack();
  
}
