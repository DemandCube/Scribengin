package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

//TODO(anthony): 1. dump info like lock or fail buffer...
public class S3SinkSourceExperimentationTest {

  private static S3Client s3Client;

  private String bucketName;
  private String folderName;

  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client();
    s3Client.onInit();
  }

  @AfterClass
  public static void afterClass() {
    s3Client.onDestroy();
  }

  @Before
  public void before() {
    bucketName = "sink-source-test-" + UUID.randomUUID();
    folderName = "integration-test";
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.createBucket(bucketName);
    s3Client.createS3Folder(bucketName, folderName);

    s3Client.createS3Folder(bucketName, folderName + "/stream-0");
    s3Client.createS3Folder(bucketName, folderName + "/stream-1");
  }

  @After
  public void after() {
    s3Client.deleteBucket(bucketName, true);
  }

  @Test
  public void testSink() throws Exception {
    StorageDescriptor sinkDescriptor = new StorageDescriptor();
    sinkDescriptor.attribute("s3.bucket.name", bucketName);
    sinkDescriptor.attribute("s3.storage.path", folderName);

    S3Sink sink = generateSink(sinkDescriptor, 0);

    assertNotNull(sink.getSinkFolder());
    // SinkStream stream = sink.newStream();
    SinkStream[] streams = sink.getStreams();
    assertEquals(2, streams.length);

    for (SinkStream sinkStream : streams) {
      assertNotNull(sinkStream.getDescriptor());
      assertNotNull(sinkStream.getWriter());
    }

    S3Util.listObjects(s3Client,bucketName);
    sink.close();
  }

  @Test
  public void testSource() throws Exception {
    int recordsPerStream = 100;
    int recordCount = 0;

    StorageDescriptor storageDescriptor = new StorageDescriptor("s3", bucketName);
    storageDescriptor.attribute("s3.bucket.name", bucketName);
    storageDescriptor.attribute("s3.storage.path", folderName);
    Sink sink = generateSink(storageDescriptor, recordsPerStream);

    S3Source source = new S3Source(s3Client, storageDescriptor);
    SourceStream[] streams = source.getStreams();
    assertEquals(1, streams.length);

    for (int i = 0; i < streams.length; i++) {
      SourceStream stream = streams[i];
      SourceStreamReader reader = stream.getReader(stream.getDescriptor().getLocation());

      while (reader.next() != null) {
        recordCount++;
      }
      reader.close();
      S3Util.listObjects(s3Client, bucketName);
      int expected = sink.getStreams().length * recordsPerStream;
      assertEquals(expected, recordCount);
    }
  }

  /*
   * Create an S3 sink and populate each stream with recordsPerStream.
   */
  private S3Sink generateSink(StorageDescriptor storageDescriptor, int recordsPerStream) throws Exception {
    S3Sink s3Sink = new S3Sink(s3Client, storageDescriptor);
    SinkStream[] streams = s3Sink.getStreams();

    for (SinkStream sinkStream : streams) {
      SinkStreamWriter writer = sinkStream.getWriter();
      for (int j = 0; j < recordsPerStream; j++) {
        String key = "stream=" + sinkStream.getDescriptor().getId() + ",buffer=" + j + ",record=" + j;
        writer.append(Record.create(key, key));
      }
      writer.commit();
      writer.close();
    }
    return s3Sink;
  }
}