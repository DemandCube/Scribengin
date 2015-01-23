package com.neverwinterdp.scribengin.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.hdfs.sink.HDFSSink;
import com.neverwinterdp.scribengin.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamReader;
import com.neverwinterdp.util.FileUtil;

/**
 * @author Tuan Nguyen
 */
public class SourceUnitTest {
  static String DATA_DIRECTORY = "./build/hdfs" ;
  
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist(DATA_DIRECTORY, false);
    fs = FileSystem.getLocal(new Configuration()) ;
    HDFSSink sink = new HDFSSink(fs, DATA_DIRECTORY);
    for(int i = 0; i < 3; i++) {
      DataGenerator.generateNewStream(sink, 3, 5);
    }
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void testSource() throws Exception {
    HDFSUtil.dump(fs, DATA_DIRECTORY);
    HDFSSource source = new HDFSSource(fs, DATA_DIRECTORY);
    SourceStream[] stream = source.getStreams();
    for(int  i = 0; i < stream.length; i++) {
      SourceStreamReader reader = stream[i].getReader("test") ;
      Record record  = null ;
      System.out.println("stream " + stream[i].getDescriptor().getId());
      while((record = reader.next()) != null) {
        System.out.println("  " + record.getKey());
      }
      reader.close();
    }
  }
}
