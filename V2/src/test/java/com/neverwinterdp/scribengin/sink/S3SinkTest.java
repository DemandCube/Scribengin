package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkListner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkTest.
 */
public class S3SinkTest {
  
  /** The s3. */
  private AmazonS3 s3;

  /**
   * Test s3 sink stream.
   *
   * @throws IOException the IO exception
   */
  @Test
  public void testS3SinkStream() throws IOException {
    String localTmpDir = "/tmp";
    String bucketName = "kafka-bucket";
    String topic = "topicTest";
    int partition = 1;
    int offsetPerPartition = 1000;
    // create the partitioner
    SinkPartitioner sp = new OffsetPartitioner(offsetPerPartition, localTmpDir, bucketName, topic, partition);
    //create the sink
    final S3SinkStream sink = new S3SinkStream(sp, bucketName, localTmpDir, Regions.US_WEST_2, 5);
    // set the max of tuples in memory to 10
    sink.setMaxTupplesInMemory(10);
    // if the number of tuples in memory  reached 10, then the callback will execute the bufferToDisk method
    sink.addOnMaxTuplesInMomoryListner(new SinkListner() {

      @Override
      public void run() {
        sink.bufferToDisk();

      }
    });
    // the max tuples stored on disk are 20
    sink.setMaxTupplesInDisk(20);
    // if the number of tuples on disk reach 20 then the the callback will run and execute the commit process
    sink.addOnMaxTuplesInDiskListner(new SinkListner() {

      @Override
      public void run() {
        sink.prepareCommit();
        sink.commit();
        sink.completeCommit();

      }
    });
    // adding tuples to sink
    int i = 0;
    for (i = 0; i < 20; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry(
          "key", i, i))));
    }

    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }

    s3 = new AmazonS3Client(credentials);
    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
    s3.setRegion(usWest2);
    //check if one of the file exist
    S3Object s3Object1 = s3.getObject(bucketName, "topicTest/1/offset=0/0_4");
    assertTrue(s3Object1 != null);


  }
}
