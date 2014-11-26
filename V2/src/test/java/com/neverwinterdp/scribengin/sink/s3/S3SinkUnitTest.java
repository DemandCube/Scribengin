package com.neverwinterdp.scribengin.sink.s3;

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
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class S3SinkUnitTest {
  Partitioner partitionner;
  private AmazonS3 s3;

  @Test
  public void testSink() throws Exception {
    String bucketName = "my-first-s3-bucket-cfa5e5d0-433f-4980-b47e-2ef99c57aa8a";
    String topic = "topicTest";
    int partition = 2;
    int offset = 1;
    partitionner = new Partitioner(bucketName, topic, partition, offset, ".txt");
    Sink sink = new S3SinkImpl(partitionner);
    AWSCredentials credentials = null;
    try {
      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ", e);
    }

    s3 = new AmazonS3Client(credentials);
    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
    s3.setRegion(usWest2);
    test(sink);
    S3Object s3Object1 = s3.getObject(bucketName, "topicTest/offset=1/2_00000000000000000001.txt");
    s3.deleteObject(bucketName, "topicTest/offset=1/2_00000000000000000001.txt");
    Assert.assertNotNull(s3Object1);

  }

  private void test(Sink sink) throws Exception {

    SinkStream stream = sink.newSinkStream();
    SinkStreamWriter streamWriter = stream.getWriter();
    for (int j = 0; j < 10; j++) {
      streamWriter.append(createRecord("key-" + "-" + j, 32));
    }

    streamWriter.commit();
    sink.close();
  }

  private Record createRecord(String key, int size) {
    byte[] data = new byte[size];
    Record record = new Record(key, data);
    return record;
  }
}
