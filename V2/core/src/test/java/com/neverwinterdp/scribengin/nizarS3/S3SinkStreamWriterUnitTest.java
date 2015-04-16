package com.neverwinterdp.scribengin.nizarS3;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.Md5Utils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.nizarS3.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.nizarS3.sink.S3SinkStreamWriter;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.util.PropertyUtils;

/**
 * The Class S3SinkTest.
 */

public class S3SinkStreamWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  /** The s3. */
  protected static AmazonS3 s3;

  /** The s3 sink config. */
  protected static S3SinkConfig s3SinkConfig;

  /** The sink. */
  protected static S3SinkStreamWriter sink;

  protected String bucketName;

  /**
   * Initialize the s3 module.
   * 
   * @param propFilePath
   *          the prop file path
   */
  protected void init(String propFilePath) {
    StreamDescriptor descriptor = new PropertyUtils(propFilePath).getDescriptor();
    descriptor.setLocation("");
    Injector injector = Guice.createInjector(new S3TestModule(descriptor, true));
    sink = injector.getInstance(S3SinkStreamWriter.class);
    s3 = injector.getInstance(AmazonS3.class);
    s3SinkConfig = injector.getInstance(S3SinkConfig.class);
    bucketName = s3SinkConfig.getBucketName();
  }

  @Test(expected = FileNotFoundException.class)
  public void testUploadNonExistentFile() throws Exception {
    init("s3.default.properties");
    for (int i = 0; i < 2; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    File file = new File(s3SinkConfig.getLocalTmpDir() + "/10/0_1");
    file.delete();
    sink.prepareCommit();
    sink.commit();
  }

  @Test
  public void testUploadSmallFile() throws Exception {
    init("s3.default.properties");
    int tuples = s3SinkConfig.getChunkSize();// ensure its 1 file
    for (int i = 0; i < tuples; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    sink.prepareCommit();
    sink.commit();
    sink.completeCommit();
    ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(), "10");
    assertTrue(list.getObjectSummaries().size() == 1);
    S3Object s3object = s3.getObject(bucketName, "10/0_1");
    assertNotNull(s3object);
  }

  // upload 10 small to a bucket
  @Test
  public void testUploadManyFilesToManyBuckets() throws Exception {
    StreamDescriptor descriptor = new PropertyUtils("s3.default.properties").getDescriptor();
    descriptor.setLocation("");
    Injector injector = Guice.createInjector(new S3TestModule(descriptor, true));
    SinkStreamWriter sink1 = injector.getInstance(S3SinkStreamWriter.class);
    s3SinkConfig = injector.getInstance(S3SinkConfig.class);
    s3 = injector.getInstance(AmazonS3.class);
    for (int i = 0; i < 8; i++) {
      sink1.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    sink1.prepareCommit();
    sink1.commit();
    ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(), "10");
    assertTrue(list.getObjectSummaries().size() == 4);
    for (int i = 0; i < 8; i += 2) {
      S3Object s3object = s3.getObject(s3SinkConfig.getBucketName(), "10/" + i + "_" + (i + 1));
      assertNotNull(s3object);
    }

    descriptor.put("bucketName", "nellouze2");
    injector = Guice.createInjector(new S3TestModule(descriptor, true));
    SinkStreamWriter sink2 = injector.getInstance(S3SinkStreamWriter.class);
    for (int i = 0; i < 8; i++) {
      sink2.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    sink2.prepareCommit();
    sink2.commit();
    list = s3.listObjects(s3SinkConfig.getBucketName(), "10");
    assertTrue(list.getObjectSummaries().size() == 4);
    for (int i = 0; i < 8; i += 2) {
      S3Object s3object = s3.getObject(s3SinkConfig.getBucketName(), "10/" + i + "_" + (i + 1));
      assertNotNull(s3object);
    }
  }

  @Test
  public void testUploadManyFiles() throws Exception {
    init("s3.default.properties");

    for (int i = 0; i < 8; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    List<String> md5s = new ArrayList<>();
    for (String file : sink.getBuffer().getFiles()) {
      md5s.add(new String(Md5Utils.md5AsBase64(new File(file))));
    }
    sink.prepareCommit();
    sink.commit();
    ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(), "10");
    assertTrue(list.getObjectSummaries().size() == 4);
    for (int i = 0; i < 8; i += 2) {
      S3Object s3object = s3.getObject(s3SinkConfig.getBucketName(), "10/" + i + "_" + (i + 1));
      assertNotNull(s3object);
    }
  }

  @Test
  public void testSimpleRollBack() throws Exception {
    init("s3.default.properties");

    for (int i = 0; i < 8; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    sink.prepareCommit();
    sink.commit();
    sink.completeCommit();
    ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(), "10");
    assertTrue(list.getObjectSummaries().size() == 4);
    for (int i = 0; i < 8; i += 2) {
      S3Object s3object = s3.getObject(s3SinkConfig.getBucketName(), "10/" + i + "_" + (i + 1));
      assertNotNull(s3object);
    }
    sink.rollback();
    S3Object s3object = null;
    try {
      s3object = s3.getObject(s3SinkConfig.getBucketName(), "10");
    } catch (AmazonS3Exception e) {

    }
    assertNull(s3object);

  }

  @Test
  public void testInteruptCommit() throws Exception {
    // commit in thread, wait a minute, interupt, logback, files in bucket
    // should be ==0;
    init("s3.default.properties");
    int tuples = s3SinkConfig.getChunkSize() * 10;
    for (int i = 0; i < tuples; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }

    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        try {
          sink.commit();
        } catch (Exception e) {
          System.out.println("we were interupted? " + e);
        }
      }
    };

    Thread thread = new Thread(runnable);
    thread.start();

    try {
      Thread.sleep(2000);
      thread.interrupt();
    } catch (Exception e) {
      e.printStackTrace();
    }
    sink.rollback();
    assertTrue(s3.listObjects(bucketName).getObjectSummaries().size() == 0);
  }

  /**
   * Records count limited.
   * 
   * @throws Exception
   */

  @Test
  public void tuplesCountLimited() throws Exception {

    init("s3.tuplesCountLimited.properties");
    for (int i = 0; i < 8; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    sink.prepareCommit();
    sink.commit();
    sink.completeCommit();
    checkFilesExist();

  }

  /**
   * Records time limited.
   * 
   * @throws Exception
   */

  @Test
  public void tuplesTimeLimited() throws Exception {

    init("s3.tuplesTimeLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      sink.append(new Record(Integer.toString(i), new byte[1024]));
      Thread.sleep(1000);
    }
    sink.prepareCommit();
    sink.commit();
    sink.completeCommit();
    checkFilesExist();
  }

  /**
   * Records size limited.
   * 
   * @throws Exception
   */

  @Test
  public void tuplesSizeLimited() throws Exception {

    init("s3.tuplesSizeLimited.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      sink.append(new Record(Integer.toString(i), new byte[1024]));
    }
    sink.prepareCommit();
    sink.commit();
    sink.completeCommit();
    checkFilesExist();
  }

  /**
   * Test rollback.
   * 
   * @throws Exception
   */

  @Test
  public void testRollback() throws Exception {

    init("s3.default.properties");
    int i = 0;
    for (i = 0; i < 8; i++) {
      sink.append(new Record(Integer.toString(i), Integer.toString(i).getBytes()));
    }
    sink.prepareCommit();
    sink.commit();
    sink.rollback();

  }

  /**
   * Check files exist.
   * 
   * @throws InterruptedException
   */

  public void checkFilesExist() {
    ObjectListing list = s3.listObjects(s3SinkConfig.getBucketName(), "20");
    assertTrue(list.getObjectSummaries().size() == 4);
    for (int i = 0; i < 8; i += 2) {
      S3Object s3object = s3.getObject(s3SinkConfig.getBucketName(), "20/" + i + "_" + (i + 1));
      assertNotNull(s3object);
    }

  }

  protected String readableFileSize(long size) {
    if (size <= 0) return "0";
    final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
    int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
    return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
  }

  public int sizeof(Object obj) throws IOException {

    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

    objectOutputStream.writeObject(obj);
    objectOutputStream.flush();
    objectOutputStream.close();

    return byteOutputStream.toByteArray().length;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected static void setEnv(Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
          .getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }
}
