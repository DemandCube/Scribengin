
package com.neverwinterdp.scribengin.storage.s3.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class S3SourceStream implements SourceStream {
  private S3Client s3Client ;
  private StreamDescriptor descriptor ;
  
  public S3SourceStream(S3Client s3Client, StreamDescriptor descriptor) {
    this.s3Client= s3Client;
    this.descriptor = descriptor;
  }
  
  public StreamDescriptor getDescriptor() { return descriptor ; }
  
  @Override
  public SourceStreamReader getReader(String name) throws FileNotFoundException, IllegalArgumentException, IOException {
    return new S3SourceStreamReader(name, s3Client, descriptor) ;
  }
}
