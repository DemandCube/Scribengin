package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Singleton;

@Singleton
public class S3Client {
  private AmazonS3Client s3Client ;
  
  @PostConstruct
  public void onInit() {
    s3Client = new AmazonS3Client();
    Region region = Region.getRegion(Regions.US_WEST_1);
    s3Client.setRegion(region);
  }

  @PreDestroy
  public void onDestroy() {
    s3Client.shutdown();
  }
  
  public AmazonS3Client getAmazonS3Client() { return this.s3Client ; }

  public Bucket createBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
    return s3Client.createBucket(bucketName) ;
  }
  
  public boolean hasBucket(String bucketName) throws AmazonClientException, AmazonServiceException { 
    return s3Client.doesBucketExist(bucketName) ; 
  }
  
  public void deleteBucket(String bucketName, boolean recursive) throws AmazonClientException, AmazonServiceException {
    if(recursive) {
      deleteKeyWithPrefix(bucketName, "");
    }
    DeleteBucketRequest request = new DeleteBucketRequest(bucketName) ;
    s3Client.deleteBucket(request);
  }
  
  public void create(String bucketName, String key, byte[] data, String mimeType) throws AmazonServiceException {
    InputStream is = new ByteArrayInputStream(data);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(mimeType);
    s3Client.putObject(new PutObjectRequest(bucketName, key, is, metadata));
  }
  
  public boolean hasKey(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
    try {
      s3Client.getObjectMetadata(bucketName, key); 
    } catch(AmazonServiceException e) {
      if(e.getStatusCode() == 404) return false;
      throw e ;
    }
    return true;
  }
  
  public ObjectMetadata getObjectMetadata(String bucketName, String key) throws AmazonClientException, AmazonServiceException { 
    return s3Client.getObjectMetadata(bucketName, key) ; 
  }
  
  public void createObject(String bucketName, String key, byte[] data, ObjectMetadata metadata) {
    InputStream is = new ByteArrayInputStream(data) ;
    createObject(bucketName, key, is, metadata);
  }
  
  public void createObject(String bucketName, String key, InputStream is, ObjectMetadata metadata) {
    s3Client.putObject(new PutObjectRequest(bucketName, key, is, metadata));
    metadata.addUserMetadata("transaction", "test");
  }
  
  public void updateObjectMetadata(String bucketName, String key, ObjectMetadata metadata) {
    CopyObjectRequest request = 
      new CopyObjectRequest(bucketName, key, bucketName, key).withNewObjectMetadata(metadata);
    s3Client.copyObject(request);
  }
  
  public S3Object getObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException { 
    return s3Client.getObject(bucketName, key) ; 
  }
  
  public S3Folder createS3Folder(String bucketName, String folderPath) throws AmazonClientException, AmazonServiceException {
    if(!hasBucket(bucketName)) {
      throw new AmazonServiceException("Bucket " + bucketName + " does not exist") ;
    }
    create(bucketName, folderPath, new byte[0], "s3system/folder");
    return new S3Folder(this, bucketName, folderPath) ;
  }
  
  public void deleteS3Folder(String bucketName, String folderPath) {
    deleteKeyWithPrefix(bucketName, folderPath);
  }
  
  public S3Folder getS3Folder(String bucketName, String folderPath) throws AmazonClientException, AmazonServiceException {
    if(!hasBucket(bucketName)) {
      throw new AmazonServiceException("Bucket " + bucketName + " does not exist") ;
    }
    if(getObjectMetadata(bucketName,folderPath) == null) {
      throw new AmazonServiceException("Folder " + folderPath + " does not exist") ;
    }
    return new S3Folder(this, bucketName, folderPath) ;
  }

  
  
  void deleteKeyWithPrefix(String bucketName, String prefix) {
    for (S3ObjectSummary file : s3Client.listObjects(bucketName, prefix).getObjectSummaries()){
      s3Client.deleteObject(bucketName, file.getKey());
    }
  }
}
