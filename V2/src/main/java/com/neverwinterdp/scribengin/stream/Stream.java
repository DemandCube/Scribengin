package com.neverwinterdp.scribengin.stream;

import com.neverwinterdp.scribengin.stream.streamdescriptor.StreamDescriptor;

public interface Stream {
  public boolean prepareCommit();
  public boolean commit();
  public boolean clearBuffer();
  public boolean completeCommit();
  public boolean rollBack();
  
  public StreamDescriptor getStreamDescriptor();
}
