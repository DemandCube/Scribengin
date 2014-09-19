package com.neverwinterdp.scribengin.scribeworker;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.scribengin.UnitTestCluster;
import com.neverwinterdp.scribengin.commitlog.ScribeCommitLog;
import com.neverwinterdp.scribengin.commitlog.ScribeCommitLogTestFactory;
import com.neverwinterdp.scribengin.scribeworker.ScribeWorker;
import com.neverwinterdp.scribengin.scribeworker.config.ScribeWorkerConfig;


//@RunWith(PowerMockRunner.class)
//@PrepareForTest({ ScribeCommitLog.class })
public class ScribeWorkerTest {
  private static String MINI_CLUSTER_PATH = "/tmp/miniCluster";

  //public ScribeConsumerTest(String name)
  //{
    //super(name);
  //}

  //public static Test suite()
  //{
    //return new TestSuite( ScribeConsumerTest.class );
  //}

  private FileSystem getMiniCluster() {
    FileSystem fs = null;
    try {
      fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();
    } catch (IOException e) {
      assert(false); //wtf?
    }
    return fs;
  }

  private String getPreCommitDirStr(ScribeWorker consumer) {
    String r = null;
    try {
      Field field = ScribeWorker.class.getDeclaredField("PRE_COMMIT_PATH_PREFIX");
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


  //@Ignore
  @Test
  public void testGetLatestOffsetFromCommitLog__corrupted_log_file()
    throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException, NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close

    // create a log with bad checksum
    log = ScribeCommitLogTestFactory.instance().build();
    ScribeCommitLogTestFactory.instance().addCorruptedEntry(
        log, 23, 33,
        "/src/path/data.2", "/dest/path/data.2", true);

    ScribeWorker sc = new ScribeWorker();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeWorker.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset =  (Long) mthd.invoke(sc);
    Assert.assertTrue(offset==22);
  }

  @Test
  public void testGetLatestOffsetFromCommitLog__data_has_been_committed()
    throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException, NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    log.record(11, 22, "/src/path/data.1", "/dest/path/data.1"); //fs is close
    ScribeWorker sc = new ScribeWorker();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeWorker.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset =  (Long) mthd.invoke(sc);
    Assert.assertTrue(offset==22);
  }

  @Test
  public void testGetLatestOffsetFromCommitLog__tmpDataFile_does_not_match_log()
    throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException, NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    log.record(11, 22, "/tmp/scribe.data.1", "/dest/path/data.1"); //fs is close
    FileSystem fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();

    String mismatchedPath = "/tmp/scribe.data.mismatched";
    FSDataOutputStream os = fs.create(new Path(mismatchedPath));
    os.write("dummy data".getBytes());
    os.write('\n');
    try {
      os.close();
    } catch (IOException e) {
      assert(false);
    }

    Assert.assertTrue(fs.exists(new Path(mismatchedPath)));

    ScribeWorkerConfig c = new ScribeWorkerConfig("hdfs://dummy", 0, "dummytopic");
    ScribeWorker sc = new ScribeWorker(c);
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));
    
    
    Method mthd = ScribeWorker.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset =  (Long) mthd.invoke(sc);
    Assert.assertTrue(offset==22);
    
    // make sure that the data file is cleaned up
    fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();
    Assert.assertFalse(fs.exists(new Path(mismatchedPath)));
  }

  @Test
  public void testGetLatestOffsetFromCommitLog__commit_uncommitted_tmp_data()
    throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchAlgorithmException, NoSuchMethodException, InvocationTargetException, Exception
  {
    ScribeCommitLog log = ScribeCommitLogTestFactory.instance().build();

    String uncommittedDataPath = "/tmp/scribe.data.1";
    String committedDataPath = "/tmp/scribe.data.1.committed";
    log.record(11, 22, uncommittedDataPath, committedDataPath); //fs is close
    FileSystem fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();

    FSDataOutputStream os = fs.create(new Path(uncommittedDataPath));
    os.write("dummy data".getBytes());
    os.write('\n');
    try {
      os.close();
    } catch (IOException e) {
      assert(false);
    }

    Assert.assertTrue(fs.exists(new Path(uncommittedDataPath)));

    ScribeWorker sc = new ScribeWorker();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    Method mthd = ScribeWorker.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset =  (Long) mthd.invoke(sc);
    Assert.assertTrue(offset==22);

    //make sure that the data file is moved
    fs = UnitTestCluster.instance(MINI_CLUSTER_PATH).build();
    Assert.assertFalse(fs.exists(new Path(uncommittedDataPath)));
    Assert.assertTrue(fs.exists(new Path(committedDataPath)));
  }


}

