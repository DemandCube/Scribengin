package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import junit.framework.Assert;

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
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.OffsetPartionner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class S3SinkTest {
  private AmazonS3 s3;

  @Test
  public void testStdOutSinkStream() throws IOException {
    String bucketName = "kafka-bucket";
    String topic = "topicTest";
    int partition = 1;

    final S3SinkStream sink = new S3SinkStream(bucketName, Regions.US_WEST_2,5, 20, 60);
    sink.addOnMaxTuplesNumberListner(new SinkListner() {
      
      @Override
      public void run() {
        sink.bufferToDisk();
        
      }
    });
    SinkPartitioner sp = new OffsetPartionner(bucketName, topic, partition, ".log");
    sink.setSinkPartitioner(sp);
    int i = 0;
    for (i = 0; i < 10; i++) {
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry(
          "key", i, i))));
    }
    assertEquals(10L, sink.getBufferSize());

    assertTrue(sink.prepareCommit());
    assertTrue(sink.commit());
    assertTrue(sink.completeCommit());

    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }
/*
    s3 = new AmazonS3Client(credentials);
    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
    s3.setRegion(usWest2);
    S3Object s3Object1 = s3.getObject(bucketName, "topicTest/0_9");
    assertTrue(s3Object1 != null);
    s3.deleteObject(bucketName, "topicTest/0_9");*/
    

  }
}
