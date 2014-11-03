package com.neverwinterdp.scribengin.scribecommitlog;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;



//import junit.framework.Test;
//import junit.framework.TestCase;
//import junit.framework.TestSuite;
import org.junit.Test;
//import org.junit.Ignore;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.commitlog.ScribeCommitLog;
import com.neverwinterdp.scribengin.commitlog.ScribeLogEntry;

public class ScribeCommitLogTest {

  private static String COMMIT_LOG_PATH = "/scribeTestCommit.log";

  //@Ignore
  @Test
  public void testRecord()
  {
    try {
      ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();
      log.record(11, 22, "/src/path/data.111", "/dest/path/data.222"); //fs is close

      log = ScribeCommitLogTestFactory.instance().build();
      log.read();

      ScribeLogEntry entry = log.getLatestEntry();
      System.out.println(">>>> " + entry.getDestPath());

    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      ScribeCommitLogTestFactory.instance().deleteCommitLog();
    }
  }

  //@Ignore
  @Test
  public void testZeroEntry()
  {
    try {
      ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();
      log.read();
      ScribeLogEntry entry = log.getLatestEntry();
      assert( entry == null);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      ScribeCommitLogTestFactory.instance().deleteCommitLog();
    }
  }

  //@Ignore
  @Test
  public void testOneEntry()
  {
    try {
      ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();
      log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close

      log = ScribeCommitLogTestFactory.instance().build();
      log.read();

      ScribeLogEntry entry = log.getLatestEntry();
      assert(entry.getStartOffset() == 11);
      assert(entry.getEndOffset() == 22);

      entry = log.getLatestEntry();
      assert (entry == null);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      ScribeCommitLogTestFactory.instance().deleteCommitLog();
    }
  }

  //@Ignore
  @Test
  public void testTwoEntry()
  {
    try {
      ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();
      log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close
      log = ScribeCommitLogTestFactory.instance().build();
      log.record(23, 33, "/src/path/data.2", "/dest/path/data.2"); //fs is close

      log = ScribeCommitLogTestFactory.instance().build();
      log.read();

      ScribeLogEntry entry = log.getLatestEntry();
      assert(entry.getStartOffset() == 23);
      assert(entry.getEndOffset() == 33);

      entry = log.getLatestEntry();
      assert(entry.getStartOffset() == 11);
      assert(entry.getEndOffset() == 22);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      ScribeCommitLogTestFactory.instance().deleteCommitLog();
    }
  }

  //@Ignore
  @Test
  public void testInvalidChecksum__TwoEntries()
  {
    try {
      ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();
      log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close
      log = ScribeCommitLogTestFactory.instance().build();
      //log.record(23, 33, "/src/path/data.2", "/dest/path/data.2"); //fs is close

      ScribeLogEntry badEntry = new ScribeLogEntry(23, 33, "/src/path/data.1", "/dest/path/data.1");
      Field field = ScribeLogEntry.class.getDeclaredField("checksum");
      field.setAccessible(true);
      byte[] badCheckSum = "DEADBEEF".getBytes();
      field.set(badEntry, badCheckSum);

      FSDataOutputStream os;
      Field fsField = ScribeCommitLog.class.getDeclaredField("fs");
      fsField.setAccessible(true);
      FileSystem fs;
      fs = (FileSystem) fsField.get(log);
      if (fs.exists(new Path(COMMIT_LOG_PATH))) {
        os = fs.append(new Path(COMMIT_LOG_PATH));
      } else {
        os = fs.create(new Path(COMMIT_LOG_PATH));
      }

      String jsonStr = ScribeLogEntry.toJson(badEntry);

      os.write(jsonStr.getBytes());
      os.write('\n');

      try {
        os.close();
      } catch (IOException e) {
      }

      log = ScribeCommitLogTestFactory.instance().build();
      log.read();

      ScribeLogEntry logEntry = log.getLatestEntry();
      assert(logEntry.getEndOffset() == 33);
      assert(logEntry.isCheckSumValid() == false);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } finally {
      ScribeCommitLogTestFactory.instance().deleteCommitLog();
    }
  }

  private void _testInvalidChecksumImp( boolean withNewline )
  {
    try {
      ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();
      log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close
      log = ScribeCommitLogTestFactory.instance().build();

      ScribeCommitLogTestFactory.instance().addCorruptedEntry(
          log, 23, 33,
          "/src/path/data.2", "/dest/path/data.2", true);

      log.read();

      ScribeLogEntry logEntry = log.getLatestEntry();
      assert(logEntry.isCheckSumValid() == false);
      assert(logEntry.getSrcPath().equals("/src/path/data.2"));

      assert(logEntry.getDestPath().equals("/dest/path/data.2"));

      logEntry = log.getLatestEntry(); // read the next entry
      assert(logEntry.isCheckSumValid() == true);
      assert(logEntry.getStartOffset() == 11);
      assert(logEntry.getEndOffset() == 22);

    } catch (IOException e) {
      e.printStackTrace();
      assert(false);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      assert(false);
    } finally {
      ScribeCommitLogTestFactory.instance().deleteCommitLog();
    }

  }

  //@Ignore
  @Test
  public void testInvalidChecksum__with_newline()
  {
    _testInvalidChecksumImp(true);
  }

  //@Ignore
  @Test
  public void testInvalidChecksum__without_newline()
  {
    _testInvalidChecksumImp(false);
  }
}
