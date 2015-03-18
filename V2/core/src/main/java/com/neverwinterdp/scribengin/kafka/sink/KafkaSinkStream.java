package com.neverwinterdp.scribengin.kafka.sink;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class KafkaSinkStream implements SinkStream {
  private StreamDescriptor descriptor;
  
  public KafkaSinkStream(StreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  @Override
  public StreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new KafkaSinkStreamWriter(descriptor);
  }
}
