package com.neverwinterdp.scribengin.s3.sink;

import com.neverwinterdp.scribengin.s3.S3Client;
import com.neverwinterdp.scribengin.s3.S3Folder;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class S3Sink implements Sink {
  private S3Client       s3Client;
  private SinkDescriptor descriptor ;
  private S3Folder       sinkFolder ;
  
  public S3Sink(S3Client s3Client, SinkDescriptor  descriptor) {
    this.s3Client = s3Client;
    this.descriptor = descriptor;
    String bucketName = descriptor.attribute("s3.bucket.name");
    String folderPath = descriptor.attribute("s3.folder.path");
    sinkFolder = s3Client.getS3Folder(bucketName, folderPath);
  }
  
  public S3Folder getSinkFolder() { return this.sinkFolder ; }
  
  @Override
  public SinkDescriptor getDescriptor() { return descriptor; }

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
