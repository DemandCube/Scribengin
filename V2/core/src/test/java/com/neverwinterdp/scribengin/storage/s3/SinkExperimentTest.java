package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

  @Test
  public void testSink() throws Exception {
    S3Util.listObjects(s3Client, bucketName);
    System.out.println("------------------");
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
      writer.commit();
      System.out.println("during the write.");
      S3Util.listObjects(s3Client, bucketName);
      System.out.println("----------------------");
    }
    writer.close();

    System.out.println("At the end of it all");
    S3Util.listObjects(s3Client, bucketName);
    System.out.println("-------------------");
  }

  @Test
  public void testRollback() throws Exception {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderName);

    S3Sink sink = new S3Sink(s3Client, descriptor);

    SinkStream stream0 = sink.newStream();
    SinkStreamWriter writer = stream0.getWriter();
    for (int i = 0; i < 100; i++) {
      writer.append(Record.create("key-" + i, "record " + i));
    }
    System.out.println("\nbefore roll back");
    S3Util.listObjects(s3Client, bucketName);

    writer.rollback();

    System.out.println("\nafter roll back");
    S3Util.listObjects(s3Client, bucketName);
    System.out.println();

    writer.close();
    System.out.println("at the end of it all");
    S3Util.listObjects(s3Client, bucketName);
    System.out.println("-------------------");
  }

  @Test
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
      S3Util.listObjects(s3Client, bucketName);
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
