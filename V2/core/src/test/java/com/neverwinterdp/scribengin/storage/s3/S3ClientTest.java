package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;

//this is a unit test but it is not named so so as not to run in jenkins
public class S3ClientTest {
  
  S3Client s3Client;
  String BUCKET_NAME = "amusyoki";
  String Folder_PATH = "unittest";

  @Before
  public void setup() {
    s3Client = new S3Client();
    s3Client.onInit();

    if (s3Client.hasBucket(BUCKET_NAME)) {
      s3Client.deleteBucket(BUCKET_NAME, true);
    }
    s3Client.createBucket(BUCKET_NAME);
    s3Client.createS3Folder(BUCKET_NAME, Folder_PATH);
    s3Client.createS3Folder(BUCKET_NAME, Folder_PATH + "/stream-0");
    s3Client.createS3Folder(BUCKET_NAME, Folder_PATH + "/stream-1");
  }

  @Test
  public void testGetRootFolders() {
    S3Folder s3Folder = new S3Folder(s3Client, BUCKET_NAME, Folder_PATH);
    for(S3ObjectSummary object : s3Folder.getChildren()) {
      System.err.println("key " + object.getKey());
    }

    List<S3Folder> folders = s3Client.getRootFolders(BUCKET_NAME);
    System.out.println("folders " + folders);
    assertEquals(1, folders.size());
  }
}