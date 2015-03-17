package com.neverwinterdp.scribengin.s3.sink;

import com.neverwinterdp.scribengin.s3.S3Folder;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class S3SinkStream implements SinkStream {
  private S3Folder streamS3Folder ;
  private SinkStreamDescriptor descriptor;
  
  public S3SinkStream(S3Folder sinkS3Folder, SinkStreamDescriptor descriptor) {
    this.descriptor = descriptor;
    String streamName = descriptor.attribute("s3.stream.name");
    streamS3Folder = sinkS3Folder.getS3Folder(streamName);
  }
  
  @Override
  public SinkStreamDescriptor getDescriptor() { return descriptor; }

  @Override
  public void delete() throws Exception {
  }

  @Override
  public SinkStreamWriter getWriter() throws Exception {
    return new S3SinkStreamWriter(streamS3Folder);
  }
}
