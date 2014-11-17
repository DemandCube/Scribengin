package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.stream.Stream;

public interface Scribe {
  void setStream(Stream s);
  
  Stream getStream();
  
  boolean init();
  boolean close();
  
  void start();
  void stop();
  
}
