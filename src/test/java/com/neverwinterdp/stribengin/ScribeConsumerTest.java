package com.neverwinterdp.stribengin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribeCommitLog;
import com.neverwinterdp.scribengin.ScribeConsumer;
//import junit.framework.Test;
//import junit.framework.TestCase;
//import junit.framework.TestSuite;


//@RunWith(PowerMockRunner.class)
//@PrepareForTest({ ScribeCommitLog.class })
public class ScribeConsumerTest {
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

    //PowerMockito.

    //FileSystem fs2 = getMiniCluster();
    //PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fs2);
    //ScribeConsumer testConsumer = PowerMockito.spy(new ScribeConsumer());
    //PowerMockito.doReturn(getMiniCluster()).when(testConsumer, "getFS");


    ScribeConsumer sc = new ScribeConsumer();
    sc.setScribeCommitLogFactory(ScribeCommitLogTestFactory.instance());
    sc.setFileSystemFactory(UnitTestCluster.instance(MINI_CLUSTER_PATH));

    ////ScribeConsumer mockSc = PowerMockito.spy(sc);

    //// add some dummy data file in the tmp directory
    //String preCommitDir = getPreCommitDirStr(sc);
    //FileSystem fs = getMiniCluster();
    //fs.create(new Path(preCommitDir + "/scribe.data.1"));
    //fs.close();

    //Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog", (Class<?>)null);
    //ScribeCommitLog testLog = ScribeCommitLogTestFactory.instance().build();
    //PowerMockito.whenNew(ScribeCommitLog.class).withArguments(Mockito.anyString()).thenReturn(testLog);
    //Mockito.spy(new ScribeCommitLog(Mockito.any(String.class)));


    Method mthd = ScribeConsumer.class.getDeclaredMethod("getLatestOffsetFromCommitLog");
    mthd.setAccessible(true);
    long offset =  (Long) mthd.invoke(sc);
    System.out.println(">> offset: " + offset);
  }

}
