package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

/**
 * @author Anthony Musyoki
 */

public class SourceExperimentTest {
  private static S3Client s3Client;

  private String bucketName;
  private int numStreams = 5;
  private int numRecordsPerStream = 10;

  @BeforeClass
  public static void setupClass() {
    s3Client = new S3Client();
    s3Client.onInit();
  }

  @AfterClass
  public static void tearDownClass() {
    s3Client.onDestroy();
  }

  @Before
  public void setup() throws Exception {
    bucketName = "source-experimenttest-" + UUID.randomUUID();
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.createBucket(bucketName);
    new S3SourceGenerator().generateSource(s3Client, bucketName, numStreams, numRecordsPerStream);
  }

  @After
  public void teardown() throws Exception {
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
  }

  @Test
  public void testSource() throws Exception {
    S3Util.listObjects(s3Client, bucketName);

    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    S3Source source = new S3Source(s3Client, descriptor);

    SourceStream[] streams = source.getStreams();
    assertEquals(1, streams.length);
    int recordCount = 0;
    for (SourceStream stream : streams) {
      SourceStreamReader reader = stream.getReader("test");
      Record record;
      System.out.println("stream " + stream.getDescriptor().getId());
      while ((record = reader.next()) != null) {
        System.out.println("  " + record.getKey());
        recordCount++;
      }
      reader.close();
    }
    assertEquals(numStreams * numRecordsPerStream, recordCount);
    S3Util.listObjects(s3Client, bucketName);
  }
}
