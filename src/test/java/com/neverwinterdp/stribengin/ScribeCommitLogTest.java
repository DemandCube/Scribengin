package com.neverwinterdp.stribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

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

  //private MiniDFSCluster createMiniCluster() {
    //File baseDir = new File("/tmp/hdfs/miniClister").getAbsoluteFile();
    //FileUtil.fullyDelete(baseDir);
    //Configuration conf = new Configuration();
    //conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    //MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);

    //MiniDFSCluster hdfsCluster = null;
    //try {
      //hdfsCluster = builder.build();
      //hdfsCluster.getFileSystemk
    //} catch (IOException e) {
    //}
    //return hdfsCluster;
  //}

  public void testRecord()
  {
    try {
      MiniDFSCluster miniCluster = UnitTestCluster.createMiniDFSCluster("/tmp/miniCluster", 1);

      FileSystem fs = miniCluster.getFileSystem();
      FSDataOutputStream os = fs.create(new Path("/testme.foo"));
      os.write("hello world".getBytes());
      os.write('\n');
      os.close();

      FSDataInputStream in = fs.open(new Path("/testme.foo"));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String currline;
      while ( (currline = br.readLine()) != null ) {
        System.out.println(">>>>>>"  + currline);
      }

    } catch (IOException e) {

    }

    //try {
      //ScribeCommitLog log = new ScribeCommitLog("/tmp/scribeTestCommit.log", true);
      //log.record(11, 22, "/src/path/data.111", "/dest/path/data.222");

    //} catch (IOException e) {
      //e.printStackTrace();
    //} catch (NoSuchAlgorithmException e) {
      //e.printStackTrace();
    //}
  }


}
