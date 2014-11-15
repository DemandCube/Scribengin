package com.neverwinterdp.scribengin.source;

import com.neverwinterdp.scribengin.tuple.Tuple;

public interface SourceStream {
  Tuple readNext();
  boolean openStream();
  boolean closeStream();
  boolean hasNext();

}
