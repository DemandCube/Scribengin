package com.neverwinterdp.scribengin.storage.s3;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.S3ObjectWriter;
import com.neverwinterdp.util.IOUtil;

public class S3ClientIntegrationTest {
  static public String BUCKET_NAME = "scribengin-" + UUID.randomUUID();
  
  static S3Client s3Client ;
  
  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client() ;
    s3Client.onInit();
    s3Client.createBucket(BUCKET_NAME);
  }
  
  @AfterClass
  static public void afterClass() {
    s3Client.deleteBucket(BUCKET_NAME, true);
    s3Client.onDestroy();
  }
  
  @Test
  public void testS3ObjectWriter() throws IOException, InterruptedException {
    String KEY = "test-s3-object-writer" ;
    S3ObjectWriter writer = new S3ObjectWriter(s3Client, BUCKET_NAME, KEY, new ObjectMetadata());
    for(int i = 0; i < 10; i++) {
      writer.write(("This is the test " + i + "\n").getBytes());
      Thread.sleep(1000);
    }
    writer.waitAndClose(10000);;
    S3Object object = s3Client.getObject(BUCKET_NAME, KEY);
    String content = IOUtil.getStreamContentAsString(object.getObjectContent(), "UTF-8") ;
    System.out.println(content);
    Assert.assertEquals(10, content.split("\n").length);
  }
  
  @Test
  public void testUpdateObjectMetadata() throws IOException, InterruptedException {
    String KEY = "test-update-objec-metadata" ;
    ObjectMetadata metadata = new ObjectMetadata() ;
    metadata.setContentType("text/plain");
    s3Client.createObject(BUCKET_NAME, KEY, new byte[0], metadata);
    metadata = s3Client.getObjectMetadata(BUCKET_NAME, KEY) ;
    metadata.addUserMetadata("transaction", "buffering");
    s3Client.updateObjectMetadata(BUCKET_NAME, KEY, metadata);
    metadata = s3Client.getObjectMetadata(BUCKET_NAME, KEY) ;
    Assert.assertEquals("buffering", metadata.getUserMetaDataOf("transaction"));
  }
  
  //TODO(anthony) this test fails
  @Test
  public void testGetRootFolders() {
    String folderPath= "unittest";

    int counter = 3;
    for (int i = 0; i < counter; i++) {
      s3Client.createS3Folder(BUCKET_NAME, folderPath + "/" + i);
    }

    List<S3Folder> folders = s3Client.getRootFolders(BUCKET_NAME);
    System.out.println("folders " + folders);
    assertEquals(counter, folders.size());
  }
  
  //TODO (anthony) test S3ObjectReader
}