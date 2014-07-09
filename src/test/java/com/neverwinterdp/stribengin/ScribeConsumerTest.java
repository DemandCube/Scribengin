package com.neverwinterdp.stribengin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.neverwinterdp.scribengin.ScribeCommitLog;
import com.neverwinterdp.scribengin.ScribeConsumer;


//@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class})
public class ScribeConsumerTest extends TestCase {
  private static String MINI_CLUSTER_PATH = "/tmp/miniCluster";

  public ScribeConsumerTest(String name)
  {
    super(name);
  }

  public static Test suite()
  {
    return new TestSuite( ScribeConsumerTest.class );
  }

  private FileSystem getMiniCluster() {
    FileSystem fs = null;
    try {
      MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster(MINI_CLUSTER_PATH, 1);
      fs = miniCluster.getFileSystem();
    } catch (IOException e) {
      assert(false); //wtf?
    }
    return fs;
  }

  private String getPreCommitDirStr(ScribeConsumer consumer) {
    String r = null;
    try {
      Field field = ScribeConsumer.class.getDeclaredField("PRE_COMMIT_PATH_PREFIX");
      field.setAccessible(true);
      r = (String) field.get(consumer);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
      assert(false);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      assert(false);
    }
    return r;
  }


  public void testGetLatestOffsetFromCommitLog__corrupted_log_file()
    throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException, NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().createCommitLog();
    log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close

    // create a log with bad checksum
    log = ScribeCommitLogTestFactory.instance().createCommitLog();
    ScribeCommitLogTestFactory.instance().addCorruptedEntry(
        log, 23, 33,
        "/src/path/data.2", "/dest/path/data.2", true);

    PowerMockito.mockStatic(FileSystem.class);
    //FileSystem mockfs = getMiniCluster();
    PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(getMiniCluster());

    ScribeConsumer sc = new ScribeConsumer();
    //ScribeConsumer mockSc = PowerMockito.spy(sc);

    // add some dummy data file in the tmp directory
    String preCommitDir = getPreCommitDirStr(sc);
    FileSystem fs = getMiniCluster();
    fs.create(new Path(preCommitDir + "/scribe.data.1"));
    fs.close();

    Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog", (Class<?>)null);
    mthd.setAccessible(true);
    Integer offset = (Integer) mthd.invoke(sc);
    System.out.println(">> offset: " + offset);
  }

}
