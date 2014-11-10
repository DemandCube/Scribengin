package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.scribe.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.source.SourceStream;

public interface Scribe {
  void setSource(SourceStream s);
  void setSink(SinkStream s);
  void setPartitioner(SinkPartitioner s);
  void processNext();
  
}
