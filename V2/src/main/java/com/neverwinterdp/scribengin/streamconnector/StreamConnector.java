package com.neverwinterdp.scribengin.streamconnector;

import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.task.Task;

public interface StreamConnector {
  
  void setSourceStream(SourceStream s);
  void setSink(SinkStream s);
  void setInvalidSink(SinkStream s);
  void setTask(Task t);
  
  SinkStream getSinkStream();
  SinkStream getInvalidSink();
  SourceStream getSourceStream();
  Task getTask();
  
  boolean processNext();
}