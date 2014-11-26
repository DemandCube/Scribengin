package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;
import com.neverwinterdp.scribengin.tuple.counter.TupleCounter;

public interface Scribe {
  
  void setTupleCounter(TupleCounter t);
  void setSourceStream(SourceStream s);
  void setSink(SinkStream s);
  void setInvalidSink(SinkStream s);
  void setTask(Task t);
  
  SinkStream getSinkStream();
  SinkStream getInvalidSink();
  SourceStream getSourceStream();
  Task getTask();
  
  boolean processNext();
  
  TupleCounter getTupleTracker();
  
  boolean init();
  
  void start();
  void stop();
  
}