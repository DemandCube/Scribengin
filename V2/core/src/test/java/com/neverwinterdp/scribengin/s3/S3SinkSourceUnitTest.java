package com.neverwinterdp.scribengin.s3;

import org.junit.BeforeClass;

public class S3SinkSourceUnitTest {
  static S3Client s3Client ;
  
  @BeforeClass
  static public void beforeClass() {
    s3Client = new S3Client() ;
    s3Client.onInit();
  }
  
  @BeforeClass
  static public void afterClass() {
    s3Client.onDestroy();
  }
}
