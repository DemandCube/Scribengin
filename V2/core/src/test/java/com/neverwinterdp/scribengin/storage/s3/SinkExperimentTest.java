package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class SinkExperimentTest {

  private static S3Client s3Client;

  private String bucketName;
  private String folderName;

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
    bucketName = "sink-unittest-" + UUID.randomUUID();
    folderName = "folder-" + UUID.randomUUID();
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

  //TODO: please print out the dump structure , before , at the middle , and after to show that 
  //you have the writing buffer, none after commit or close. All the buffer should merge to a data file.
  @Test
  public void testSink() throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderName);

    S3Sink sink = new S3Sink(s3Client, descriptor);
    SinkStream[] streams = sink.getStreams();
    assertEquals(0, streams.length);

    SinkStream stream = sink.newStream();
    assertEquals(1, sink.getStreams().length);

    SinkStreamWriter writer = stream.getWriter();
    
    
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 100; j++) {
        String key = "stream=" + stream.getDescriptor().getId() + ",buffer=" + i + ",record=" + j;
        writer.append(Record.create(key, key));
      }
      // writer.commit();
    }
    writer.commit();
    writer.close();

    S3Util.listObjects(s3Client, bucketName);
  }
  
  
  //TODO: for rollback I think you just need to discard the uncommit buffer. Is it hard to implement with S3
  @Test
  @Ignore("rollback not implemented in writer")
  public void testRollback() throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderName);

    S3Sink sink = new S3Sink(s3Client, descriptor);

    SinkStream stream0 = sink.newStream();
    SinkStreamWriter writer = stream0.getWriter();
    for (int i = 0; i < 10; i++) {
      writer.append(Record.create("key-" + i, "record " + i));
    }
    writer.rollback();
    writer.close();
    S3Util.listObjects(s3Client, bucketName);
  }

  @Test
  public void testAutoCreateFalse() throws Exception {
   String newBucket = "sink-unittest-" + UUID.randomUUID();
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", newBucket);
    descriptor.attribute("s3.storage.path", folderName);
    try {
      S3Sink sink = new S3Sink(s3Client, descriptor);
      sink.close();
      fail("Auto create is false. Test should fail");
    } catch (Exception e) {
      //success
      return;
    }
  }  
  
  @Test
  public void testAutoCreateTrue() throws Exception {
    String newBucket = "sink-unittest-" + UUID.randomUUID();
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", newBucket);
    descriptor.attribute("s3.bucket.autocreate", String.valueOf(true));
    descriptor.attribute("s3.storage.path", folderName);
    try {
      S3Sink sink = new S3Sink(s3Client, descriptor);
      System.out.println(sink.hashCode());
      s3Client.deleteBucket(newBucket, true);
    } catch (Exception e) {
       fail("Auto create is true. Bucket should have been created.");
    }
  }

  //TODO: why ?? when you have multi thread , each thread will write to its own buffer. What do you mean 
  //correctly implemented
  @Test
  @Ignore // untill writer.commit is correctly implemented
  public void testMultiThread() throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderName);
    S3Sink sink = new S3Sink(s3Client, descriptor);

    SinkStreamWriterTask[] task = new SinkStreamWriterTask[5];
    ExecutorService service = Executors.newFixedThreadPool(task.length);
    for (int i = 0; i < task.length; i++) {
      service.execute(new SinkStreamWriterTask(sink));
    }
    service.shutdown();
    while (!service.isTerminated()) {
      System.out.println("----------------------------------------");
      Thread.sleep(2000);
    }
  }

  public class SinkStreamWriterTask implements Runnable {
    private Sink sink;

    public SinkStreamWriterTask(Sink sink) {
      this.sink = sink;
    }

    @Override
    public void run() {
      try {
        SinkStream stream = sink.newStream();
        SinkStreamWriter writer = stream.getWriter();
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
          for (int j = 0; j < 100; j++) {
            writer.append(Record.create("key-" + i, "record " + i));
            Thread.sleep(random.nextInt(10));
          }
          writer.commit();
        }
        writer.close();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
