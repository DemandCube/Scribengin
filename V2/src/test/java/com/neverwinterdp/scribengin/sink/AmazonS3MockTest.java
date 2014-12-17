package com.neverwinterdp.scribengin.sink;


import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.neverwinterdp.scribengin.sink.AmazonS3Mock.ExceptionType;

public class AmazonS3MockTest {

  @Before
  public void setUp() throws Exception {
  }

  @Test(expected=AmazonClientException.class)
  public void testCreateBucketException() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateCreateBucketException(ExceptionType.AmazonClientException);
    s3sinkMock.createBucket("bucketName");
  }
  
  @Test(expected=AmazonClientException.class)
  public void testDoesBucketExist() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateDoesBucketExistException(ExceptionType.AmazonClientException,false);
    s3sinkMock.doesBucketExist("bucketName");
  }
  
  @Test(expected=AmazonClientException.class)
  public void testGetBucketAcl() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateGetBucketAclException(ExceptionType.AmazonClientException);
    s3sinkMock.getBucketAcl("bucketName");
  }
  
  @Test(expected=AmazonClientException.class)
  public void testPutObject() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulatePutObjectException(ExceptionType.AmazonClientException);
    PutObjectRequest object = new PutObjectRequest("bucketName", "key", new File("test",""));
    s3sinkMock.putObject(object);
  }
  

}
