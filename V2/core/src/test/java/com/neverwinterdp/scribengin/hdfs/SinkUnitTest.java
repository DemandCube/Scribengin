package com.neverwinterdp.scribengin.hdfs;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.hdfs.sink.HDFSSink;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.util.FileUtil;

public class SinkUnitTest {
  static String DATA_DIRECTORY = "./build/sinkhdfs" ;
  
  private FileSystem fs ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist(DATA_DIRECTORY, false);
    fs = FileSystem.getLocal(new Configuration()) ;
  }
  
  @After
  public void teardown() throws Exception {
    fs.close();
  }
  
  @Test
  public void testSink() throws Exception {
    HDFSSink sink = new HDFSSink(fs, DATA_DIRECTORY);
    SinkStream[] streams = sink.getStreams();
    Assert.assertEquals(0, streams.length);
    
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 5; i++) {
      for(int j = 0; j < 100; j ++) {
        String key = "stream=" + stream.getDescriptor().getId() +",buffer=" + i + ",record=" + j;
        writer.append(DataGenerator.createRecord(key, key));
      }
      writer.commit();
    }
    System.out.println("Before close");
    HDFSUtil.dump(fs, DATA_DIRECTORY);
    writer.close();
    System.out.println("\n\n") ;
    System.out.println("After close");
    HDFSUtil.dump(fs, DATA_DIRECTORY);
  }
  
  @Test
  public void testRollback() throws Exception {
    HDFSSink sink = new HDFSSink(fs, DATA_DIRECTORY);
    SinkStream stream0 = sink.newStream();
    SinkStreamWriter writer = stream0.getWriter();
    for(int i = 0; i < 10; i ++) {
      writer.append(DataGenerator.createRecord("key-" + i, "record " + i));
    }
    writer.rollback();
    writer.close();
    HDFSUtil.dump(fs, DATA_DIRECTORY);
  }
  
  @Test
  public void testMultiThread() throws Exception {
    HDFSSink sink = new HDFSSink(fs, DATA_DIRECTORY);
    SinkStreamWriterTask[] task = new SinkStreamWriterTask[5]; 
    ExecutorService service = Executors.newFixedThreadPool(task.length);
    for(int i = 0; i < task.length; i++) {
      service.execute(new SinkStreamWriterTask(sink));
    }
    service.shutdown();
    while(!service.isTerminated()) {
      HDFSUtil.dump(fs, DATA_DIRECTORY);
      System.out.println("----------------------------------------");
      Thread.sleep(2000);
    }
    HDFSUtil.dump(fs, DATA_DIRECTORY);
  }
  
  public class SinkStreamWriterTask implements Runnable {
    private Sink sink ;
    
    public SinkStreamWriterTask(Sink sink) {
      this.sink = sink ;
    }
    
    @Override
    public void run() {
      try {
        SinkStream stream = sink.newStream();
        SinkStreamWriter writer = stream.getWriter();
        Random random = new Random() ;
        for(int i = 0; i < 5; i++) {
          for(int j = 0; j < 100; j ++) {
            writer.append(DataGenerator.createRecord("key-" + i, "record " + i));
            Thread.sleep(random.nextInt(10));
          }
          writer.commit();
        }
        writer.close();
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
