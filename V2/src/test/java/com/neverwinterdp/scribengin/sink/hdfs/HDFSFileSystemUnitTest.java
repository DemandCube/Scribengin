package com.neverwinterdp.scribengin.sink.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import com.neverwinterdp.util.IOUtil;

public class HDFSFileSystemUnitTest {
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
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
    //TODO: create file1 and file2, test concat file1 file2
  }
}
