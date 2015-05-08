package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.tool.message.MessageExtractor;
import com.neverwinterdp.tool.message.MessageTracker;

/**
 * @author Anthony Musyoki
 */

public class SourceExperimentTest {
  private static S3Client s3Client;

  private String bucketName;
  private int numStreams = 1;
  private int numOfBuffersPerStream =10;
  private int numRecordsPerBuffer = 100;

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
    new S3SourceGenerator().generateSource(s3Client, bucketName, numStreams, numOfBuffersPerStream, numRecordsPerBuffer);
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

    MessageTracker messageTracker = new MessageTracker();
    MessageExtractor messageExtractor = MessageExtractor.DEFAULT_MESSAGE_EXTRACTOR;

    SourceStream[] streams = source.getStreams();
    assertEquals(1, streams.length);
    for (SourceStream stream : streams) {
      SourceStreamReader reader = stream.getReader("test");
      Record record;
      System.out.println("stream " + stream.getDescriptor().getId());
      while ((record = reader.next()) != null) {
        Message message = messageExtractor.extract(record.getData());
        messageTracker.log(message);
      }
      reader.close();
    }
    messageTracker.optimize();
    messageTracker.dump(System.out);
    int totalRecords= numStreams * numOfBuffersPerStream * numRecordsPerBuffer;
   assertEquals(totalRecords, messageTracker.getLogCount());
   assertTrue(messageTracker.isInSequence());
    
    S3Util.listObjects(s3Client, bucketName);
  }
}
