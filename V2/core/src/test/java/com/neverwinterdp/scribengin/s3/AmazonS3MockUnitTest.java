package com.neverwinterdp.scribengin.s3;


import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.neverwinterdp.scribengin.s3.AmazonS3Mock.ExceptionType;

public class AmazonS3MockUnitTest {

  @Before
  public void setUp() throws Exception {
  }

  @Test(expected=AmazonClientException.class)
  public void testCreateBucketException() {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.simulateCreateBucketException(ExceptionType.AmazonClientException);
    s3sinkMock.createBucket("test");
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
    s3sinkMock.putObject("bucketName", "key", new File("test",""));
  }
  
  @Test
  public void testPutObject() throws IOException {
    AmazonS3Mock s3sinkMock = new AmazonS3Mock();
    s3sinkMock.createBucket("test_bucket");
    File file = new File("test","");
    file.createNewFile();
    PutObjectResult result = s3sinkMock.putObject("test_bucket", "key", file);
    file.delete();
    assertTrue(result != null);
  }
  

}