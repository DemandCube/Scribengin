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
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.s3.source.S3Source;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

/**
 * @author Anthony Musyoki
 */

//TODO(team)  for S3Source a sourceStream is a folder, for S3Sink a sinkStream is a file in a folder.
public class SourceExperimentTest {
  private static S3Client s3Client;

  private String bucketName;

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
    // delete bucket, create bucket
    bucketName = "source-unittest-" + UUID.randomUUID();
    if (s3Client.hasBucket(bucketName)) {
      s3Client.deleteBucket(bucketName, true);
    }
    s3Client.createBucket(bucketName);
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
    int numStreams = 5;
    int numRecordsPerStream=10;
    generateTestData(numStreams, numRecordsPerStream);
    
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    S3Source source = new S3Source(s3Client, descriptor);
    
    SourceStream[] streams = source.getStreams();
    assertEquals(numStreams, streams.length);
    int recordCount=0;
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

  /*
   * Create an S3 sink and populate each stream with recordsPerStream.
   */
  private void generateTestData(int numStreams, int recordsPerStream) throws Exception {
    //TODO: Pay attention to method name , variable name... In this loop, you are trying to
    //create stream , why do you name   String folderName = "sourcetest-" + i; the folder name is the sink name
    //or stream name. What is the point to create a StorageDescriptor in the loop. Do you plan to create 
    //a new Sink for each loop iteration ?
    //Your code is hard to read and follow. I would expect a generate method to generate a sink with a number of 
    //stream. Here I do not know what and where you create the sink and it is easy to have the bug. 
    //I don't know how it works with this method s3Client.createS3Folder(bucketName, folderName); how can it create and
    //recreate without any problem.
    //The testSink method take about 15s , but the testSource method that use this method takes more than 50s.
    for (int i = 0; i < numStreams; i++) {
      String folderName = "sourcetest-" + i;
      StorageDescriptor descriptor = new StorageDescriptor();
      descriptor.attribute("s3.bucket.name", bucketName);
      descriptor.attribute("s3.storage.path", folderName);

      s3Client.createS3Folder(bucketName, folderName);
      s3Client.createS3Folder(bucketName, folderName + "/stream-0");

      S3Sink s3Sink = new S3Sink(s3Client, descriptor);
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
    }
    System.out.println("after writing ");
    S3Util.listObjects(s3Client, bucketName);
  }
}
