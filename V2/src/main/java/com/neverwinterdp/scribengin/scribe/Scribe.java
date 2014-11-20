package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.streamconnector.StreamConnector;

public interface Scribe {
  void setStream(StreamConnector s);
  
  StreamConnector getStream();
  
  boolean init();
  boolean close();
  
  void start();
  void stop();
  
}
