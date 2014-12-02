package com.neverwinterdp.scribengin.scribe.state;

public enum ScribeState {
  UNINITIALIZED,
  INIT,
  BUFFERING,
  PREPARING_COMMIT,
  COMMITTING,
  CLEARING_BUFFER,
  COMPLETING_COMMIT,
  ROLLINGBACK,
  ERROR,
  STOPPED,
}
