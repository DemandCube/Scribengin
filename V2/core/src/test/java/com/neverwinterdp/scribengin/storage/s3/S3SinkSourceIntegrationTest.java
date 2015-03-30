package com.neverwinterdp.scribengin.storage.s3;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3SinkSourceIntegrationTest {
  static public String BUCKET_NAME ;
  static public String STORAGE_PATH = "database";
  
  static S3Client s3Client ;
  
  @BeforeClass
  static public void beforeClass() {
    BUCKET_NAME = "sink-source-test-" + UUID.randomUUID(); 
    s3Client = new S3Client() ;
    s3Client.onInit();
    if(s3Client.hasBucket(BUCKET_NAME)) {
      s3Client.deleteBucket(BUCKET_NAME, true);
    }
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createS3Folder(BUCKET_NAME, STORAGE_PATH);
    //TODO: You may need to remove this and really create a stream with some data 
    //in order the source can work properly
    s3Client.createS3Folder(BUCKET_NAME, STORAGE_PATH + "/stream-0");
    s3Client.createS3Folder(BUCKET_NAME, STORAGE_PATH + "/stream-1");
  }
  
  @AfterClass
  static public void afterClass() {
    s3Client.deleteBucket(BUCKET_NAME, true);
    s3Client.onDestroy();
  }
  
  @Test
  public void testSink() throws Exception {
    StorageDescriptor sinkDescriptor = new StorageDescriptor() ;
    sinkDescriptor.attribute("s3.bucket.name",  BUCKET_NAME);
    sinkDescriptor.attribute("s3.storage.path", STORAGE_PATH);
    S3Sink sink = new S3Sink(s3Client, sinkDescriptor);
    Assert.assertNotNull(sink.getSinkFolder());
    SinkStream stream3 = sink.newStream() ;
    SinkStream[] streams = sink.getStreams() ;
    Assert.assertEquals(3, streams.length);
    
    for(int i = 0; i < 3; i++) {
      SinkStreamWriter writer = stream3.getWriter();
      for(int j = 0; j < 100; j ++) {
        String key = "stream=" + stream3.getDescriptor().getId() +",buffer=" + i + ",record=" + j;
        writer.append(Record.create(key, key));
      }
      writer.commit();
      writer.close() ;
    }
  }
}