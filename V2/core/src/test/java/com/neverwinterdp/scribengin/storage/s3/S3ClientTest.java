package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//this is a unit test but it is not named so so as not to run in jenkins
public class S3ClientTest {

  private S3Client s3Client;
  private final String BUCKET_NAME = "amusyoki";
  private final String FOLDER_PATH = "unit-test";

  @Before
  public void setup() {
    s3Client = new S3Client();
    s3Client.onInit();

    if (s3Client.hasBucket(BUCKET_NAME)) {
      s3Client.deleteBucket(BUCKET_NAME, true);
    }
  }
  
  @After
  public void tearDown() {
    s3Client.deleteBucket(BUCKET_NAME, true);
  }

  @Test
  public void testGetRootFolders() {
    s3Client.createBucket(BUCKET_NAME);

    int counter = 3;
    for (int i = 0; i < counter; i++) {
      s3Client.createS3Folder(BUCKET_NAME, FOLDER_PATH + "/" + i);
    }

    List<S3Folder> folders = s3Client.getRootFolders(BUCKET_NAME);
    System.out.println("folders " + folders);
    assertEquals(counter, folders.size());
  }
}