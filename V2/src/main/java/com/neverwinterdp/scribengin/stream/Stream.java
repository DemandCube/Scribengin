package com.neverwinterdp.scribengin.stream;

import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;

public interface Stream {
  
  void setSourceStream(SourceStream s);
  void setSink(SinkStream s);
  void setInvalidSink(SinkStream s);
  void setTask(Task t);
  
  SinkStream getSinkStream();
  SinkStream getInvalidSink();
  SourceStream getSourceStream();
  Task getTask();
  
  boolean processNext();
  boolean initStreams();
  boolean closeStreams();
  
  boolean verifyDataInSink();
  boolean fixDataInSink();
}