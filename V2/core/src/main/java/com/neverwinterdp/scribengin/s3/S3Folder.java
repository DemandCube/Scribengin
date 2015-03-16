package com.neverwinterdp.scribengin.s3;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Folder {
  private AmazonS3       s3Client ;
  private String         bucketName;
  private String         folderPath;
  
  public S3Folder(AmazonS3 s3Client, String bucketName, String folderPath) {
    this.s3Client   = s3Client;
    this.bucketName = bucketName;
    if("/".equals(folderPath)) folderPath = "";
    this.folderPath = folderPath;
  }
  
  public AmazonS3 getS3Client() { return s3Client; }

  public String getBucketName() { return bucketName; }

  public String getFolderPath() { return folderPath; }
  
  public void create(String name, byte[] data, String mimeType) {
    InputStream is = new ByteArrayInputStream(data);
    create(name, is, mimeType);
  }
  
  public void create(String name, InputStream is, String mimeType) {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(mimeType);
    create(name, is, metadata);
  }
  
  public void create(String name, InputStream is, ObjectMetadata metadata) {
    s3Client.putObject(new PutObjectRequest(bucketName, toKey(name), is, metadata));
  }
  
  public List<S3ObjectSummary> getDescendants() {
    ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName).withPrefix(folderPath);
    ObjectListing objectListing = s3Client.listObjects(request);
    return objectListing.getObjectSummaries();
  }
  
  public S3Folder getS3Folder(String name) {
    return new S3Folder(s3Client, bucketName, toKey(name));
  }
  
  public S3Object getS3Object(String name) {
    S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, toKey(name)));
    return object;
  }
  
  String toKey(String name) { return folderPath + "/" + name; }
  
  public String toString() {
    return bucketName + ":" + folderPath;
  }
}
