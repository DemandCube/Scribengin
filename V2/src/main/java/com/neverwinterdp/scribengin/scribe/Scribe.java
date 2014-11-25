package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.streamconnector.StreamConnector;

public interface Scribe {
  void setStream(StreamConnector s);
  
  StreamConnector getStreamConnector();
  
  boolean init();
  boolean close();
  
  void start();
  void stop();
  
}
