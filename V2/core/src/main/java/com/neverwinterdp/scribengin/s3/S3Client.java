package com.neverwinterdp.scribengin.s3;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;

import com.google.inject.Singleton;

@Singleton
public class S3Client {
  private AmazonS3Client s3Client ;
  
  @PostConstruct
  public void onInit() {
    s3Client = new AmazonS3Client();
    Region region = Region.getRegion(Regions.DEFAULT_REGION);
    s3Client.setRegion(region);
  }

  @PreDestroy
  public void onDestroy() {
    s3Client.shutdown();
  }
  
  public AmazonS3Client getAmazonS3Client() { return this.s3Client ; }
  
  public S3Folder getS3Folder(String bucketName, String folderPath) {
    return new S3Folder(s3Client, bucketName, folderPath) ;
  }
}
