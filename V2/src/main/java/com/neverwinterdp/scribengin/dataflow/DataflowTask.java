package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.source.Source;

public interface DataflowTask {
  public DataflowTaskDescriptor getDescriptor() ;

  public Source getSource();
  public Sink getSink();

  public void execute(SourceStream[] in, SinkStream[] out) ;
}
