package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.scribe.state.ScribeState;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;
import com.neverwinterdp.scribengin.tuple.counter.TupleCounter;

public interface Scribe {
  
  public void setTupleCounter(TupleCounter t);
  public void setSourceStream(SourceStream s);
  public void setSink(SinkStream s);
  public void setInvalidSink(SinkStream s);
  public void setTask(Task t);
  
  public SinkStream getSinkStream();
  public SinkStream getInvalidSink();
  public SourceStream getSourceStream();
  public Task getTask();
  
  public boolean processNext();
  
  public TupleCounter getTupleTracker();
  
  public boolean init();
  public boolean init(ScribeState state);
  
  public void start();
  public void stop();
  
  public ScribeState getState();
  public void setState(ScribeState s);
  
  public boolean recover();
}