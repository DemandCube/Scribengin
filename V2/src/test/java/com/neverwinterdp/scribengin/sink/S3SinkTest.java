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
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class S3SinkTest.
 */
 public  class S3SinkTest {
  
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

    if(bucketName.equals("DefaultBucketName")){
      assertTrue("You need to change the default bucket Name in the sink.s3.properties", false);
    }
    
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
    SinkStream sink = new S3SinkStream(sp, s3SinkConfig);

    // adding tuples to sink
    int i = 0;
    for (i = 0; i < 20; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry(
          "key", i, i))));
    }
    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.completeCommit());
    
    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }

    s3 = new AmazonS3Client(credentials);
    Regions regionName = Regions.fromName(s3SinkConfig.getRegionName());
    Region region = Region.getRegion(regionName);
    s3.setRegion(region);
    ObjectListing list = s3.listObjects(bucketName, "topicTest/1/offset=0/");
    assertTrue(list.getObjectSummaries().size()==4);
    //check if one of the file exist
    String path;
    for(int j=0;j < 20;j+=5){
      path = "topicTest/1/offset=0/"+j+"_"+(j+4);
      S3Object s3Object1 = s3.getObject(bucketName, path );
      assertTrue(s3Object1 != null);

    }
  }
  @Test
    public void testFullBuffer() throws IOException {
      
      String topic = "topicTest";
      int kafkaPartition = 1;
      // create the partitioner
      SinkPartitioner sp = new OffsetPartitioner(offsetPerPartition, localTmpDir, bucketName, topic, kafkaPartition);
      //create the sink
      final S3SinkStream sink = new S3SinkStream(sp, s3SinkConfig);

      // adding tuples to sink
      int i = 0;
      for (i = 0; i < 40; i++) {
        assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry(
            "key", i, i))));
      }
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry(
          "key", i, i)))==false);

  }
  @Test(expected=AmazonClientException.class)
  public void testS3BadCredentials() throws IOException {
    
    String topic = "topicTest";
    int kafkaPartition = 1;
    // create the partitioner
    SinkPartitioner sp = new OffsetPartitioner(offsetPerPartition, localTmpDir, bucketName, topic, kafkaPartition);
    s3SinkConfig.setCredentialPath("");
    //create the sink
    final S3SinkStream sink = new S3SinkStream(sp, s3SinkConfig);

    
  }
}
