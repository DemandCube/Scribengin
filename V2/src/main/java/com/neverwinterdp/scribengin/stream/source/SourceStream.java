package com.neverwinterdp.scribengin.stream.source;

import com.neverwinterdp.scribengin.stream.Stream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public interface SourceStream extends Stream{
  Tuple readNext();
  boolean hasNext();
  String getName();
}
