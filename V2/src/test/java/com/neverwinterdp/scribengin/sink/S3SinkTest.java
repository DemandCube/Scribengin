package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkTest.
 */
//TODO move partitioner and buffer tests to separate classes
public class S3SinkTest {

  /** The s3. */
  private static AmazonS3 s3;

  /** The s3 sink config. */
  private static S3SinkConfig s3SinkConfig;

  /** The sink. */
  private static SinkStream sink;

  /**
   * Initialize the s3 module.
   * 
   * @param propFilePath the prop file path
   */
  public void init(String propFilePath) {
    Injector injector = Guice.createInjector(new S3Module(propFilePath, "topicTest", 1, false));
    sink = injector.getInstance(S3SinkStream.class);
    s3 = injector.getInstance(AmazonS3.class);
    s3SinkConfig = injector.getInstance(S3SinkConfig.class);
  }



  @Test(expected = AmazonClientException.class)
  public void testBadCredentials() throws IOException {

    AWSCredentials credentials = new BasicAWSCredentials("", "");
    AmazonS3Client s3Client = new AmazonS3Client(credentials);
    System.out.println(s3Client.getRegion());
  }

  @Test(expected = FileNotFoundException.class)
  public void testUploadNonExistentFile() {
    init("s3.tuplesCountLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(),
          new CommitLogEntry(
              "key", i, i))));
    }
    String path;
    for (int j = 0; j < 8; j += 2) {
      path = +j + "_" + (j + 1);
      File f = new File(s3SinkConfig.getLocalTmpDir() + "/topicTest/1/offset=0/" + path);
      f.delete();
    }
    sink.commit();
  }

  @Test(expected = FileNotFoundException.class)
  public void testUploadToNonExistentBucket() {

  }

  @Test(expected = FileNotFoundException.class)
  public void testUploadToNonWritableBucket() {

  }

  @Test(expected = FileNotFoundException.class)
  public void testCreateExistingBucket() {

  }

  @Test
  public void testUploadFile() {

    // assert object.getcontent.md5 = file.getcontent.md5
  }

  @Test
  public void testUploadManyFile() {

    // assert object.getcontent.md5 = file.getcontent.md5
  }

  @Test
  public void testUploadManyFileToManyBuckets() {

    // assert object.getcontent.md5 = file.getcontent.md5
  }

  @Test(expected = AmazonClientException.class)
  public void testUploadFileTwice() {

  }

  @Test
  public void InterruptCommitProcess() {
    sink.commit();
    sink.rollBack();
    // assert s3 doesn't contain file
  }


  // upload 10000 small files using 100 as offsetPerPartition and 10 as chunk size
  @Test
  public void testObeysInstallProperties() {

  }

  @Test
  // upload 100 files using 10 as chunk size
  public void testCountUploadedFiles() {

  }

  // Upload 10 5GB files to a bucket
  @Test
  public void testUploadBigFiles() {

  }

  // Upload 10 5GB files to 10 buckets (100 files total)
  @Test
  public void testUploadTenBuckets() {

  }

  // get partition for startOffset=0 and endOffset=100
  @Test
  public void testPartitioner() {

  }
  /**
   * Tuples count limited.
   * 
   * @throws IOException the IO exception
   * @throws InterruptedException
   */
  @Test
  public void tuplesCountLimited() throws IOException, InterruptedException {

    init("s3.tuplesCountLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(),
          new CommitLogEntry(
              "key", i, i))));
    }
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.completeCommit());
    checkFilesExist();

  }

  /**
   * Tuples time limited.
   * 
   * @throws IOException the IO exception
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void tuplesTimeLimited() throws IOException, InterruptedException {

    init("s3.tuplesTimeLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), new byte[1024],
          new CommitLogEntry(
              "key", i, i))));
      Thread.sleep(1000);
    }
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.completeCommit());
    checkFilesExist();
  }

  /**
   * Tuples size limited.
   * 
   * @throws IOException the IO exception
   * @throws InterruptedException
   */
  @Test
  public void tuplesSizeLimited() throws IOException, InterruptedException {

    init("s3.tuplesSizeLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), new byte[1024],
          new CommitLogEntry(
              "key", i, i))));
    }
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.completeCommit());
    checkFilesExist();
  }

  /**
   * Test rollback.
   * 
   * @throws IOException the IO exception
   */
  @Test
  public void testRollback() throws IOException {

    init("s3.tuplesCountLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(),
          new CommitLogEntry(
              "key", i, i))));
    }
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.rollBack());

  }

  /**
   * Check files exist.
   * 
   * @throws InterruptedException
   */

  public void checkFilesExist() {
    ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(), "topicTest/1/offset=0");
    assertTrue(list.getObjectSummaries().size() == 4);
    String path;
    for (int j = 0; j < 8; j += 2) {
      path = +j + "_" + (j + 1);
      S3Object s3Object1 =
          s3.getObject(s3SinkConfig.getBucketName() + "/topicTest/1/offset=0", path);
      assertTrue(s3Object1 != null);

    }
  }
}
