package com.neverwinterdp.testSink;

import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class TestSinkStream implements SinkStream{

  protected SinkStreamDescriptor sinkStreamDesc;
  
  public TestSinkStream(SinkStreamDescriptor desc){
    this.sinkStreamDesc = desc;
  }
  
  @Override
  public SinkStreamDescriptor getDescriptor() {
    return this.sinkStreamDesc;
  }

  @Override
  public void delete() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new TestSinkWriter();
  }

}
