package com.neverwinterdp.scribengin.kafka.sink;

import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class SinkStreamImpl implements SinkStream {
  private SinkStreamDescriptor descriptor;
  
  public SinkStreamImpl(SinkStreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  @Override
  public SinkStreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new SinkStreamWriterImpl(descriptor);
  }
}
