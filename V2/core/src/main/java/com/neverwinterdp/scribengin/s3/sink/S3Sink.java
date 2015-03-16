package com.neverwinterdp.scribengin.s3.sink;

import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class S3Sink implements Sink {

  @Override
  public SinkDescriptor getDescriptor() {
    return null;
  }

  @Override
  public SinkStream getStream(SinkStreamDescriptor descriptor) throws Exception {
    return null;
  }

  @Override
  public SinkStream[] getStreams() {
    return null;
  }

  @Override
  public void delete(SinkStream stream) throws Exception {
  }

  @Override
  public SinkStream newStream() throws Exception {
    return null;
  }

  @Override
  public void close() throws Exception {
  }
}
