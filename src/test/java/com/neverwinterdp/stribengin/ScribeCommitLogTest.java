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
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.neverwinterdp.scribengin.ScribeCommitLog;
import com.neverwinterdp.scribengin.ScribeLogEntry;

public class ScribeCommitLogTest extends TestCase {
  private static final Log log =
    LogFactory.getLog(ScribeCommitLogTest.class);


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
      MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster("/tmp/miniCluster", 1);
      FileSystem fs = miniCluster.getFileSystem();
      ScribeCommitLog log = new ScribeCommitLog("/scribeTestCommit.log", true);

      Field field = ScribeCommitLog.class.getDeclaredField("fs");
      field.setAccessible(true);
      field.set(log, fs);
      return log;
  }

  public void testRecord()
  {
    try {
      ScribeCommitLog log = createCommitLog();
      log.record(11, 22, "/src/path/data.111", "/dest/path/data.222"); //fs is close

      log = createCommitLog();
      log.readLastTwoEntries();

      ScribeLogEntry e = log.getLatestEntry();
      System.out.println(">>>> " + e.getDestPath());


    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

}
