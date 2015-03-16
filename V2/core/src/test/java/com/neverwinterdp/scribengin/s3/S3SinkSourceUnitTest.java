package com.neverwinterdp.scribengin.s3;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;

public class S3SinkSourceUnitTest {
  static public String BUCKET_NAME = "sink-source-test";
  static public String FOLDER_PATH = "database";
  
  static S3Client s3Client ;
  
  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client() ;
    s3Client.onInit();
    if(!s3Client.hasBucket(BUCKET_NAME)) {
      s3Client.createBucket(BUCKET_NAME);
    }
    s3Client.createS3Folder(BUCKET_NAME, FOLDER_PATH) ;
  }
  
  @AfterClass
  static public void afterClass() {
    s3Client.deleteBucket(BUCKET_NAME, true);
    s3Client.onDestroy();
  }
  
  @Test
  public void testSink() {
    SinkDescriptor sinkDescriptor = new SinkDescriptor() ;
    sinkDescriptor.attribute("s3.bucket.name", BUCKET_NAME);
    sinkDescriptor.attribute("s3.folder.path", FOLDER_PATH);
    S3Sink sink = new S3Sink(s3Client, sinkDescriptor);
    Assert.assertNotNull(sink.getSinkFolder());
  }
}