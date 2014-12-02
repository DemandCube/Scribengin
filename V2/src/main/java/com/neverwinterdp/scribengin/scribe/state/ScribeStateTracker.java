package com.neverwinterdp.scribengin.scribe.state;

public interface ScribeStateTracker {
  public ScribeState getScribeState();
  public void setState(ScribeState s);
}
