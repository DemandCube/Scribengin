package com.neverwinterdp.scribengin.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSFileSystemUnitTest {
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/hdfs", false);
    fs = FileSystem.getLocal(new Configuration()) ;
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void testCreateReadWrite() throws Exception {
    String TEXT = "hello" ;
    Path testPath = new Path("./build/hdfs/test.txt"); 
    FSDataOutputStream os = fs.create(testPath) ;
    os.write(TEXT.getBytes());
    os.close();
    
    FSDataInputStream is = fs.open(testPath);
    String text = IOUtil.getStreamContentAsString(is, "UTF-8");
    Assert.assertEquals(TEXT, text);
  }
  
  @Test
  public void testConcat() throws Exception {
    Path[] path = new Path[10];
    for(int i = 0; i < path.length; i++) {
      path[i] = new Path("./build/hdfs/file-" + i + ".txt");
      String TEXT = "file content " + i ;
      FSDataOutputStream os = fs.create(path[i]) ;
      os.write(TEXT.getBytes());
      os.close();
    }
    
    Path concatPath = new Path("./build/hdfs/concat.txt");
    try {
      fs.concat(concatPath, path);
    } catch(UnsupportedOperationException ex) {
      //TODO
      System.err.println("TODO: test concat method with real HDFS");
    }
    
    HDFSUtil.concat(fs, concatPath, path, true);
  }
}