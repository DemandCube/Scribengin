package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.s3.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.s3.sink.S3SinkStreamWriter;
import com.neverwinterdp.scribengin.util.PropertyUtils;

/**
 * The Class S3SinkTest.
 */

public class S3SinkStreamWriterIntegrationTest extends S3SinkStreamWriterUnitTest {

 
  protected void init(String propFilePath) {
    SinkStreamDescriptor descriptor = new PropertyUtils(propFilePath).getDescriptor();
    descriptor.setLocation("");
    Injector injector = Guice.createInjector(new S3TestModule(descriptor, false));
    sink = injector.getInstance(S3SinkStreamWriter.class);
    s3 = injector.getInstance(AmazonS3.class);
    s3SinkConfig = injector.getInstance(S3SinkConfig.class);
    bucketName = s3SinkConfig.getBucketName();
  }
  
  @After
  public void afterTest() {
    System.out.println("buckets " + System.getenv("AWS_CREDENTIAL_PROFILES_FILE"));
    init("s3.tuplesCountLimited.properties");
    for (S3ObjectSummary object : s3.listObjects(bucketName, "").getObjectSummaries()) {
      System.out.println("deleting " + object.getKey());
      s3.deleteObject(bucketName, object.getKey());
    }
  }
  // Upload 10 5GB files to a bucket
  @Test(expected = IllegalArgumentException.class)
  public void testUploadOneBigFiles() throws Exception {
    init("s3.largeFiles.properties");

    long chunks = 13629999L;
    Record tuple = null;

    System.out.println("file size " + readableFileSize(chunks * 394L));
    for (int i = 0; i < chunks; i++) {
      tuple = new Record(Integer.toString(i), Integer.toString(i).getBytes());

      sink.append(tuple);
    }
    sink.commit();
    assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 0);
  }

  // Attempt to upload 10 4GB files
  @Test(expected = AmazonClientException.class)
  public void testUploadTenBigFilesToTenBuckets() throws Exception {
    init("s3.4GBFiles.properties");

    long chunks = s3SinkConfig.getChunkSize() * 4;
    Record tuple = null;

    System.out.println("file size " + readableFileSize(chunks * 394L));
    for (int i = 0; i < chunks; i++) {
      tuple = new Record(Integer.toString(i), Integer.toString(i).getBytes());

      sink.append(tuple);
    }
    sink.commit();
    assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 4);
  }

  @Test(expected = AmazonS3Exception.class)
  public void testBadCredentials() throws Exception {
    Map<String, String> env = System.getenv();
    Map<String, String> fakeEnv = new ConcurrentHashMap<String, String>();
    try {
      File temp = File.createTempFile("fakeCredentials", ".tmp");
      String credentials = " #User Name=artfullyContrived \r\n" + "[default] \r\n"
          + "aws_access_key_id=FAKESTACCESSKEYEVER7338 \r\n"
          + "aws_secret_access_key=ANOTHEREXTREMELYFAKESECRETACCESSKEY1084 \r\n";

      Files.append(credentials, temp, Charsets.UTF_8);
      fakeEnv.put("AWS_CREDENTIAL_PROFILES_FILE", temp.getCanonicalPath());
      fakeEnv.putAll(env);
      setEnv(fakeEnv);

      System.out.println("credentitals file " + System.getenv("AWS_CREDENTIAL_PROFILES_FILE"));
      init("s3.tuplesCountLimited.properties");
      int tuples = s3SinkConfig.getChunkSize();
      for (int i = 0; i < tuples; i++) {
        sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
      }
      sink.commit();
    } finally {
      fakeEnv.remove("AWS_CREDENTIAL_PROFILES_FILE");
      setEnv(fakeEnv);
      System.out.println("buckets " + System.getenv("AWS_CREDENTIAL_PROFILES_FILE"));
    }
  }
}
