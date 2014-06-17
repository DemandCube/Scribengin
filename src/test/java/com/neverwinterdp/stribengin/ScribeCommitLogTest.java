package com.neverwinterdp.stribengin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.neverwinterdp.scribengin.ScribeCommitLog;
import com.neverwinterdp.scribengin.ScribeLogEntry;

public class ScribeCommitLogTest extends TestCase {
  private static final Log log =
    LogFactory.getLog(ScribeCommitLogTest.class);

  private static String MINI_CLUSTER_PATH = "/tmp/miniCluster";
  private static String COMMIT_LOG_PATH = "/scribeTestCommit.log";

  public ScribeCommitLogTest(String name)
  {
    super(name);
  }

  public static Test suite()
  {
    return new TestSuite( ScribeCommitLogTest.class );
  }

  private ScribeCommitLog createCommitLog() throws IOException ,NoSuchFieldException, IllegalAccessException
  {
    MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster(MINI_CLUSTER_PATH, 1);
    FileSystem fs = miniCluster.getFileSystem();
    ScribeCommitLog log = new ScribeCommitLog(COMMIT_LOG_PATH, true);

    Field field = ScribeCommitLog.class.getDeclaredField("fs");
    field.setAccessible(true);
    field.set(log, fs);
    return log;
  }

  private void deleteCommitLog()
  {
    try {
      MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster(MINI_CLUSTER_PATH, 1);
      FileSystem fs = miniCluster.getFileSystem();
      fs.delete(new Path(COMMIT_LOG_PATH), false);
    } catch (IOException e) {

    }
  }

  public void testRecord()
  {
    try {
      ScribeCommitLog log = createCommitLog();
      log.record(11, 22, "/src/path/data.111", "/dest/path/data.222"); //fs is close

      log = createCommitLog();
      log.readLastTwoEntries();

      ScribeLogEntry entry = log.getLatestEntry();
      System.out.println(">>>> " + entry.getDestPath());

    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      deleteCommitLog();
    }
  }

  public void testLastTwoEntries_zeroEntry()
  {
    try {
      ScribeCommitLog log = createCommitLog();
      log.readLastTwoEntries();
      ScribeLogEntry entry = log.getLatestEntry();
      assert( entry == null);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      deleteCommitLog();
    }
  }

  public void testLastTwoEntries__OneEntry()
  {
    try {
      ScribeCommitLog log = createCommitLog();
      log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close

      log = createCommitLog();
      log.readLastTwoEntries();

      ScribeLogEntry entry = log.getLatestEntry();
      assert(entry.getStartOffset() == 11);
      assert(entry.getEndOffset() == 22);

      entry = log.getLatestEntry();
      assert (entry == null);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      deleteCommitLog();
    }
  }

  public void testLastTwoEntries__TwoEntry()
  {
    try {
      ScribeCommitLog log = createCommitLog();
      log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close
      log = createCommitLog();
      log.record(23, 33, "/src/path/data.2", "/dest/path/data.2"); //fs is close

      log = createCommitLog();
      log.readLastTwoEntries();

      ScribeLogEntry entry = log.getLatestEntry();
      assert(entry.getStartOffset() == 23);
      assert(entry.getEndOffset() == 33);

      entry = log.getLatestEntry();
      assert(entry.getStartOffset() == 11);
      assert(entry.getEndOffset() == 22);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      deleteCommitLog();
    }
  }

  public void testLastTwoEntries__ThreeEntry()
  {

  }
}