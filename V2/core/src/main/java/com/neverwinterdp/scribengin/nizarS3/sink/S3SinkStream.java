package com.neverwinterdp.scribengin.nizarS3.sink;

import java.io.IOException;

import com.google.inject.Injector;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3SinkStream implements SinkStream {
  private StreamDescriptor descriptor;
  private Injector injector;

  public S3SinkStream(Injector injector, StreamDescriptor descriptor) throws IOException {
    this.descriptor = descriptor;
    this.injector = injector;
  }

  public StreamDescriptor getDescriptor() {
    return this.descriptor;
  }

  synchronized public void delete() throws Exception {
  }

  @Override
  synchronized public SinkStreamWriter getWriter() throws IOException {
    SinkStreamWriter sink = injector.getInstance(S3SinkStreamWriter.class);
    return sink;
  }

}