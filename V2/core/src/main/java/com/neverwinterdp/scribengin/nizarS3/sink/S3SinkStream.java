package com.neverwinterdp.scribengin.nizarS3.sink;

import java.io.IOException;

import com.google.inject.Injector;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class S3SinkStream implements SinkStream {
  private SinkStreamDescriptor descriptor;
  private Injector injector;

  public S3SinkStream(Injector injector, SinkStreamDescriptor descriptor) throws IOException {
    this.descriptor = descriptor;
    this.injector = injector;
  }

  public SinkStreamDescriptor getDescriptor() {
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