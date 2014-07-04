package com.neverwinterdp.stribengin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.neverwinterdp.scribengin.ScribeCommitLog;
import com.neverwinterdp.scribengin.ScribeLogEntry;

public class ScribeCommitLogTestFactory {
  private static final Log log =
    LogFactory.getLog(ScribeCommitLogTestFactory.class);

  private static ScribeCommitLogTestFactory inst = null;
  private static String MINI_CLUSTER_PATH = "/tmp/miniCluster";
  private static String COMMIT_LOG_PATH = "/scribeTestCommit.log";

  private ScribeCommitLogTestFactory() {

  }

  public static ScribeCommitLogTestFactory instance() {
    if (inst == null)
      inst = new ScribeCommitLogTestFactory();
    return inst;
  }

  public ScribeCommitLog createCommitLog() throws IOException ,NoSuchFieldException, IllegalAccessException
  {
    MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster(MINI_CLUSTER_PATH, 1);
    FileSystem fs = miniCluster.getFileSystem();
    ScribeCommitLog log = new ScribeCommitLog(COMMIT_LOG_PATH, true);

    Field field = ScribeCommitLog.class.getDeclaredField("fs");
    field.setAccessible(true);
    field.set(log, fs);
    return log;
  }

  public static void deleteCommitLog()
  {
    try {
      MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster(MINI_CLUSTER_PATH, 1);
      FileSystem fs = miniCluster.getFileSystem();
      fs.delete(new Path(COMMIT_LOG_PATH), false);
    } catch (IOException e) {

    }
  }

  private FSDataOutputStream getOutputStream(ScribeCommitLog log) {
    FSDataOutputStream os = null;
    try {
      Field fsField = ScribeCommitLog.class.getDeclaredField("fs");
      fsField.setAccessible(true);
      FileSystem fs;
      fs = (FileSystem) fsField.get(log);
      if (fs.exists(new Path(COMMIT_LOG_PATH))) {
        os = fs.append(new Path(COMMIT_LOG_PATH));
      } else {
        os = fs.create(new Path(COMMIT_LOG_PATH));
      }
      return os;
    } catch(NoSuchFieldException e) {
      e.printStackTrace();
      assert(false);
    } catch(IOException e) {
      e.printStackTrace();
      assert(false);
    } catch(IllegalAccessException e) {
      e.printStackTrace();
      assert(false);
    }
    return os;
  }

  // flush to disk
  public void addCorruptedEntry(ScribeCommitLog log, long startOffset, long endOffset,
        String srcPath, String dstPath, boolean endWithNewline)
  {
    try {
      ScribeLogEntry badEntry = new ScribeLogEntry(startOffset, endOffset, srcPath, dstPath);
      Field field = ScribeLogEntry.class.getDeclaredField("checksum");
      field.setAccessible(true);
      byte[] badCheckSum = "DEADBEEF".getBytes();
      field.set(badEntry, badCheckSum);

      FSDataOutputStream os = getOutputStream(log);
      String jsonStr = ScribeLogEntry.toJson(badEntry);
      if (endWithNewline) {
        jsonStr += "\n";
      }
      os.write(jsonStr.getBytes());

      try {
        os.close();
      } catch (IOException e) {
        e.printStackTrace();
        assert(false);
      }

    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      assert(false);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
      assert(false);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      assert(false);
    } catch (IOException e) {
      e.printStackTrace();
      assert(false);
    }
  }

  // flush to disk
  public void invalidateJSONSyntax(ScribeCommitLog log, long startOffset, long endOffset,
      String srcPath, String dstPath, boolean endWithNewline)
  {
    try {
      ScribeLogEntry badEntry = new ScribeLogEntry(startOffset, endOffset, srcPath, dstPath);

      FSDataOutputStream os = getOutputStream(log);

      String jsonStr = ScribeLogEntry.toJson(badEntry);
      jsonStr += "make it into a BAD json string.";
      if (endWithNewline) {
        jsonStr += "\n";
      }
      os.write(jsonStr.getBytes());

    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      assert(false);
    } catch (IOException e) {
      e.printStackTrace();
      assert(false);
    }
  }

}
