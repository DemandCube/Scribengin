package com.neverwinterdp.scribengin.stream.sink.config;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class S3SinkConfig extends SinkConfig {

  @Inject(optional = true) @Named("s3.bucketName")
  private String   bucketName = "buckbuck";
  
  @Override
  public void load() {
    // TODO Auto-generated method stub
    
  }

}
