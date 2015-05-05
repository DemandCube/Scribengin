package com.neverwinterdp.scribengin.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Folder {
  private S3Client       s3Client ;
  private String         bucketName;
  private String         folderPath;
  
  public S3Folder(S3Client s3Client, String bucketName, String folderPath) {
    this.s3Client   = s3Client;
    this.bucketName = bucketName;
    if("/".equals(folderPath)) folderPath = "";
    this.folderPath = folderPath;
  }
  
  public S3Client getS3Client() { return s3Client; }

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
    s3Client.getAmazonS3Client().putObject(new PutObjectRequest(bucketName, toKey(name), is, metadata));
  }
  
  public S3Folder createFolder(String name) {
    return s3Client.createS3Folder(bucketName, name) ;
  }
  
  public List<String> getChildrenNames() {
    List<String> holder = new ArrayList<String>() ;
    for(S3ObjectSummary sel : getChildren()) {
      String name = sel.getKey().substring(folderPath.length() + 1);
      holder.add(name);
    }
    return holder;
  }
  
  public List<S3ObjectSummary> getChildren() {
    ListObjectsRequest request = 
      new ListObjectsRequest().withBucketName(bucketName).withPrefix(folderPath + "/").withDelimiter("/");
    ObjectListing objectListing = s3Client.getAmazonS3Client().listObjects(request);
    return objectListing.getObjectSummaries() ;
  }
  
  public List<S3ObjectSummary> getDescendants() {
    ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName).withPrefix(folderPath + "/");
    ObjectListing objectListing = s3Client.getAmazonS3Client().listObjects(request);
    return objectListing.getObjectSummaries();
  }
  
  public S3Folder getS3Folder(String name) {
    return new S3Folder(s3Client, bucketName, toKey(name));
  }

  public void createObject(String name, byte[] data, ObjectMetadata metadata) throws IOException {
    s3Client.createObject(bucketName, toKey(name), data, metadata);
  }
  
  public void createObject(String name, InputStream is, ObjectMetadata metadata) throws IOException {
    s3Client.createObject(bucketName, toKey(name), is, metadata);
  }
  
  public void updateObjectMetadata(String name, ObjectMetadata metadata) throws IOException {
    s3Client.updateObjectMetadata(bucketName, toKey(name), metadata);
  }
  
  public S3ObjectWriter createObjectWriter(String name, ObjectMetadata metadata) throws IOException {
    S3ObjectWriter writer = new S3ObjectWriter(s3Client, bucketName, toKey(name), metadata);
   //TODO (tuan) confirm if this should be here.
    s3Client.createS3Folder(bucketName, toKey(name));
    return writer;
  }
  
  public S3Object getS3Object(String name) {
    S3Object object = s3Client.getAmazonS3Client().getObject(new GetObjectRequest(bucketName, toKey(name)));
    return object;
  }
  
  
  String toKey(String name) { return folderPath + "/" + name; }
  
  public String toString() {
    return bucketName + ":" + folderPath;
  }
}