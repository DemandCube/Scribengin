package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.BeforeClass;
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
import com.neverwinterdp.scribengin.sink.AmazonS3Mock.ExceptionType;
import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkListner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkTest.
 */
 public  class S3SinkIntegrationTest {
  
  /** The s3. */
  private AmazonS3 s3;
  private static String localTmpDir;
  private static String bucketName;
  private static int offsetPerPartition;
  private static S3SinkConfig s3SinkConfig;
  
  @BeforeClass
  public static void setup(){
    
    s3SinkConfig = new S3SinkConfig("sink.s3.properties");
    localTmpDir = s3SinkConfig.getLocalTmpDir();
    bucketName = s3SinkConfig.getBucketName();
    offsetPerPartition = s3SinkConfig.getOffsetPerPartition();
    
  }
  /**
   * Test s3 sink stream.
   *
   * @throws IOException the IO exception
   */
  @Test
  public void testS3SinkStream() throws IOException {
    
    String topic = "topicTest";
    int kafkaPartition = 1;
    // create the partitioner
    SinkPartitioner sp = new OffsetPartitioner(offsetPerPartition, localTmpDir, bucketName, topic, kafkaPartition);
    //create the sink
    final S3SinkStream sink = new S3SinkStream(sp, s3SinkConfig);
    AmazonS3Mock s3Mock = new AmazonS3Mock();
    sink.setS3Client(s3Mock);
    
    // adding tuples to sink
    int i = 0;
    for (i = 0; i < 20; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry(
          "key", i, i))));
    }
    s3Mock.simulateDoesBucketExistException(ExceptionType.AmazonClientException, true);
    assertTrue(sink.prepareCommit()==false);
    s3Mock.clear();
    
    s3Mock.simulateDoesBucketExistException(ExceptionType.AmazonClientException, true);
    s3Mock.simulateCreateBucketException(ExceptionType.AmazonClientException);
    assertTrue(sink.prepareCommit()==false);
    s3Mock.clear();
    
    s3Mock.simulateGetBucketAclException(ExceptionType.AmazonClientException);
    assertTrue(sink.prepareCommit()==false);
    s3Mock.clear();
    
    s3Mock.clear();
    s3Mock.simulatePutObjectException(ExceptionType.AmazonClientException);
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit()==false);
    
    s3Mock.clear();
    s3Mock.simulateDeleteObjectException(ExceptionType.AmazonClientException);
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.rollBack()==false);
    

  }
}
