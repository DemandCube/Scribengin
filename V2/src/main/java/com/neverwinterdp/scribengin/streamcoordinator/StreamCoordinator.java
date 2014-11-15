package com.neverwinterdp.scribengin.streamcoordinator;

import com.neverwinterdp.scribengin.scribe.Scribe;

public interface StreamCoordinator {
  public Scribe[] allocateStreams();
}
