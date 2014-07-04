package com.neverwinterdp.stribengin;

import java.io.IOException;
import java.lang.reflect.Field;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.neverwinterdp.scribengin.ScribeConsumer;

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

  public void testGetLatestOffsetFromCommitLog__corrupted_log_file() throws IOException
  {
    ScribeConsumer sc = new ScribeConsumer();
    //FileSystem fs = getMiniCluster();

    //// add some dummy data file in the tmp directory
    //String preCommitDir = getPreCommitDirStr(sc);
    //fs.create(new Path(preCommitDir + "/scribe.data.1"));
    //fs.close();

    //create a log with bad checksum

  }

}
