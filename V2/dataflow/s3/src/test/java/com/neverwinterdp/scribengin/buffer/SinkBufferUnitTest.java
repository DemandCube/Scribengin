  package com.neverwinterdp.scribengin.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.neverwinterdp.scribengin.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.util.Label;

public class SinkBufferUnitTest {

  private SinkBuffer buffer;
  S3SinkConfig config;
  SinkPartitioner partitioner;

  private String topic = "testTopic2";
  private String pathname;

  @Before
  public void setUp() {
    pathname = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + topic;
    int offsetPerPartition = 10;
    int kafkaPartition = 0;
    partitioner = new OffsetPartitioner(offsetPerPartition, topic, kafkaPartition);
    config = new S3SinkConfig("s3.default.properties");
    buffer = new SinkBuffer(partitioner, config);
  }

  @Test
  @Label("S3_19")
  public void testAdd() {
    // just to be sure
    assertEquals(0, buffer.size());
    int tuples = 100;
    Record tuple;
    for (int i = 0; i < tuples; i++) {
      tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    assertEquals(tuples, buffer.size());

    int expectedFiles = tuples / config.getChunkSize();
    // get count of files in expected folder
    int files = countFiles(new File(pathname), true);
    assertEquals(expectedFiles, files);
    // TODO make this work
    /*
     * int expectedFolders = tuples / (config.getChunkSize() * config.getOffsetPerPartition());
     * int folders = countFiles(new File(pathname), false);
     * assertEquals(expectedFolders, folders);
     */
  }

  @Test
  // This test fails for tuples > 1000000
  public void testAppendRecordsToMemory() {
    // add zero tuples to memory, get tuplesCountInMemory
    assertEquals(0, buffer.size());

    // add a million tuples to memory, get tuplesCountInMemory
    int tuples = 1000;
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    assertEquals(tuples, buffer.size());
  }


  @Test
  public void testPurgeMemoryToDisk() {
    // test how many written to disk
    // test how many remain in memory
    int tuples = 1000;
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    // we expect all tuples on disk, 0 in memory
    buffer.purgeMemoryToDisk();
    System.out.println(buffer.size());

    System.out.println(countFiles(new File(pathname), true));
    System.out.println();
    assertEquals(tuples, buffer.size());
    assertEquals(0, buffer.tuplesInMemory());
    assertEquals(tuples, buffer.tuplesOnDisk());
    System.out.println("finished");
  }

  @Test
  public void testClear() {
    // add tuples, call clear. check zero in memory, check zero in file
    int tuples = 1000;
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    try {
      buffer.clear();
    } catch (IOException e) {
      System.out.println("got an exception ");
      Assert.fail("got an exception. " + e.getMessage());
    }
    assertEquals(0, buffer.size());
    assertEquals(0, buffer.getFilesCount());
    assertTrue(buffer.isEmpty());
    assertFalse(buffer.isSaturated());
  }

  @Test
  public void testGetFilesSize() {
    int tuples = 1000;
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    int expected = tuples / config.getChunkSize();
    assertEquals(expected, buffer.getFilesCount());
  }

  @Test
  public void testPollFromDisk() {
    int tuples = 10;
    String message = "test message ";
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), (message + i).getBytes());
      buffer.add(tuple);
    }
    String actual;
    String expected = "test message 0";
    try {
      File read = buffer.pollFromDisk();
      System.out.println("exists " + read.exists());
      System.out.println("length " + read.length());
      System.out.println("total space " + read.getTotalSpace());
      actual = Files.readFirstLine(read, Charsets.UTF_8).trim();
      System.out.println(actual);
      System.out.println(expected);
      assertEquals(expected, actual);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testIsSaturated() {
    int tuples = 1000;
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    assertEquals(tuples, buffer.size());
    assertTrue(buffer.isSaturated());
  }

  @Test
  public void testGetRecordsCount() {
    int tuples = 1000;
    for (int i = 0; i < tuples; i++) {
      Record tuple = new Record(Integer.toString(i), new byte[10]);
      buffer.add(tuple);
    }
    assertEquals(tuples, buffer.size());
  }

  @After
  public void tearDown() throws Exception {
    // delete files from tmp dir
    buffer = null;
    File file = new File(pathname);
    FileUtils.deleteDirectory(file);
  }

  public int countFiles(File file, boolean filesOnly) {
    File[] files = file.listFiles();
    int count = 0;
    for (File f : files)
      if (f.isDirectory()) {
        count += countFiles(f, filesOnly);
      } else {
        count++;
      }
    return count;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

}
