package com.neverwinterdp.scribengin.storage.s3.sink;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Folder;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3SinkStream implements SinkStream {
  private S3Folder streamS3Folder ;
  private StreamDescriptor descriptor;
  
  public S3SinkStream(S3Folder sinkS3Folder, StreamDescriptor descriptor) {
    this.descriptor = descriptor;
    String streamName = descriptor.attribute("s3.stream.name");
    streamS3Folder = sinkS3Folder.getS3Folder(streamName);
  }
  
  @Override
  public StreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new S3SinkStreamWriter(streamS3Folder);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("S3SinkStream [streamS3Folder=");
    builder.append(streamS3Folder);
    builder.append(", descriptor=");
    builder.append(descriptor);
    builder.append("]");
    return builder.toString();
  }
}