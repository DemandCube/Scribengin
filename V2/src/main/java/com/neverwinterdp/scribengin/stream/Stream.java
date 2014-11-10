package com.neverwinterdp.scribengin.stream;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.task.Task;
import com.neverwinterdp.scribengin.trigger.Trigger;

public interface Stream {
  public StreamDescriptor getDescriptor() ;
  
  public Source getSource();
  public Sink getSink();
  public Task getTask();
  
  void setTask(Task t);
  void setTrigger(Trigger t);
  
  public Scribe[] getScribes();
  
  public boolean start();
  public boolean stop();
  
  public void nextTuple();
  //public void execute(SourceStream[] in, SinkStream[] out) ;
}