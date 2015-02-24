package com.neverwinterdp.scribengin.sink;


import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.neverwinterdp.scribengin.sink.AmazonS3Mock.ExceptionType;

public class AmazonS3MockTest {

  @Before
  public void setUp() throws Exception {
  }

  @Test(expected=AmazonClientException.class)
  public void testCreateBucketException() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateCreateBucketException(ExceptionType.AmazonClientException);
  }
  
  
  @Test
  public void testCreateBucket() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    Bucket bucket = s3sinkMock.createBucket("test");
    assertTrue(bucket.getName().equals("test"));
    
  } 
  @Test(expected=AmazonClientException.class)
  public void testDoesBucketExistException() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateDoesBucketExistException(ExceptionType.AmazonClientException,false);
    s3sinkMock.doesBucketExist("bucketName");
  }
  
  @Test
  public void testDoesBucketExist() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateDoesBucketExistException(ExceptionType.None,true);
    boolean result = s3sinkMock.doesBucketExist("bucketName");
    assertTrue(result);
  }
  
  @Test(expected=AmazonClientException.class)
  public void testGetBucketAclException() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateGetBucketAclException(ExceptionType.AmazonClientException);
    s3sinkMock.getBucketAcl("test");
  }
 
  @Test
  public void testGetBucketAcl() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    AccessControlList acl = s3sinkMock.getBucketAcl("test");
    assertTrue(acl != null);
    
  }
  
  @Test(expected=AmazonClientException.class)
  public void testPutObjectException() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulatePutObjectException(ExceptionType.AmazonClientException);
    PutObjectRequest object = new PutObjectRequest("bucketName", "key", new File("test",""));
    s3sinkMock.putObject(object);
  }
  
  @Test
  public void testPutObject() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    PutObjectRequest object = new PutObjectRequest("bucketName", "key", new File("test",""));
    PutObjectResult result = s3sinkMock.putObject(object);
    assertTrue(result != null);
  }
  

}
