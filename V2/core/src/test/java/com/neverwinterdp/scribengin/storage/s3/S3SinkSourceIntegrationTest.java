package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class S3SinkSourceIntegrationTest {
  static public String BUCKET_NAME;
  static public String STORAGE_PATH = "database";

  static S3Client s3Client;

  int counter = 0;

  @BeforeClass
  static public void beforeClass() {
    BUCKET_NAME = "amusyoki";
    s3Client = new S3Client();
    s3Client.onInit();
    if (s3Client.hasBucket(BUCKET_NAME)) {
      s3Client.deleteBucket(BUCKET_NAME, true);
    }
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createS3Folder(BUCKET_NAME, STORAGE_PATH);
    //TODO: You may need to remove this and really create a stream with some data 
    //in order the source can work properly
    s3Client.createS3Folder(BUCKET_NAME, STORAGE_PATH + "/stream-0");
    s3Client.createS3Folder(BUCKET_NAME, STORAGE_PATH + "/stream-1");
  }

  // @AfterClass
  static public void afterClass() {
    s3Client.deleteBucket(BUCKET_NAME, true);
    s3Client.onDestroy();
  }

  @Test
  public void testSink() throws Exception {
    StorageDescriptor sinkDescriptor = new StorageDescriptor();
    sinkDescriptor.attribute("s3.bucket.name", BUCKET_NAME);
    sinkDescriptor.attribute("s3.storage.path", STORAGE_PATH);
    S3Sink sink = new S3Sink(s3Client, sinkDescriptor);
    assertNotNull(sink.getSinkFolder());
    SinkStream stream = sink.newStream();
    SinkStream[] streams = sink.getStreams();
    assertEquals(2, streams.length);

    for (SinkStream sinkStream : streams) {
      SinkStreamWriter writer = sinkStream.getWriter();
      for (int j = 0; j < 50; j++) {
        String key = "stream=" + stream.getDescriptor().getId() + ",buffer=" + j + ",record=" + j;
        writer.append(Record.create(key, key));
        counter++;
      }
      writer.commit();
      writer.close();
    }
  }

  @Test
  public void testSource() throws Exception {
    testSink();
    int recordCount = 0;
    StorageDescriptor sourceDescriptor = new StorageDescriptor("s3", BUCKET_NAME);
    sourceDescriptor.attribute("s3.bucket.name", BUCKET_NAME);

    S3Source source = new S3Source(s3Client, sourceDescriptor);
    SourceStream[] streams = source.getStreams();
    assertEquals(1, streams.length);

    for (int i = 0; i < streams.length; i++) {
      System.out.println("int test getStreamDescriptor " + streams[i].getDescriptor());

      SourceStream stream = streams[i];
      SourceStreamReader reader = stream.getReader(stream.getDescriptor().getLocation());
      Record record;
      while ((record = reader.next()) != null) {
        System.err.println(record.getKey());
        recordCount++;
      }
      assertEquals(counter, recordCount);
      reader.commit();
      reader.close();
    }
  }

  public void readFromS3(String bucketName, String key) throws IOException {
    S3Object s3object = s3Client.getAmazonS3Client().getObject(new GetObjectRequest(
        bucketName, key));
    System.out.println("contentType " + s3object.getObjectMetadata().getContentType());
    System.out.println("contentlength " + s3object.getObjectMetadata().getContentLength());

    BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
    String line;
    while ((line = reader.readLine()) != null) {
      // can copy the content locally as well
      // using a buffered writer
      System.out.println(line);
    }
  }
}