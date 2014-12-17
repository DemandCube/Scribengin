package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.S3SinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.OffsetPartitioner;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.task.CopyTask;

public class ScribeS3Test {
  
  /** The s3. */
  private AmazonS3 s3;
  private static String localTmpDir;
  private static String bucketName;
  private static int offsetPerPartition;
  private static S3SinkConfig s3SinkConfig;
  @BeforeClass
  public static void setup(){
    
    s3SinkConfig = new S3SinkConfig("sink.s3.properties");
    localTmpDir = s3SinkConfig.getLocalTmpDir();
    bucketName = s3SinkConfig.getBucketName();
    offsetPerPartition = s3SinkConfig.getOffsetPerPartition();
    
  }
  
  @Test
  public void testScribe() throws Exception {
    int bufferLimit = 5000;
    String testDir = "./XXXXXXXXX/";
    System.err.println("Working Directory = "+System.getProperty("user.dir"));
    
    String topic = "topicTest";
    int kafkaPartition = 1;
    // create the partitioner
    SinkPartitioner sp = new OffsetPartitioner(offsetPerPartition, localTmpDir, bucketName, topic, kafkaPartition);
    //create the sink
    SinkStream sink = new S3SinkStream(sp, s3SinkConfig);
    
    Scribe scribe = new ScribeImpl(new SequentialIntSourceStream(), 
        sink, 
        sink, 
        new CopyTask(bufferLimit));

    

    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(3000);
    scribe.stop();
    Thread.sleep(1500);
    
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    
    //Make sure there are no temp files and nothing committed to invalid sink
    File[] files = new File(testDir+"/invalid/commit/").listFiles();
    assertEquals(0, files.length);
    files = new File(testDir+"tmp").listFiles();
    assertNull(files);
    files = new File(testDir+"/invalid/tmp").listFiles();
    assertNull(files);
    
    //Make sure all the files are created correctly
    //Should be a sequential list of files listed numerically
    files = new File(testDir+"commit/").listFiles(); 
    Arrays.sort(files);
    LinkedList<Integer> listOfCommittedFiles = new LinkedList<Integer>();
    for(File f: files){
      if(!f.getName().startsWith(".")){
        listOfCommittedFiles.add(Integer.parseInt(f.getName()));
      }
    }
    Collections.sort(listOfCommittedFiles);
    
    //Make sure its written enough files
    assertTrue(listOfCommittedFiles.size() > 2);
    
    for(int i = 0; i < listOfCommittedFiles.size(); i++){
      
      if(i == listOfCommittedFiles.size() - 1){
        //We're at the last file and can't guarantee that this file has a full 5000 entries
        break;
      }
      
      //Make sure file exists
      assertEquals(new Integer(i), listOfCommittedFiles.get(i));
      //Make sure data is correct
      Path path = FileSystems.getDefault().getPath(testDir+"commit/", Integer.toString(i));
      String expectedData = "";
      for(int j = 1; j <= bufferLimit; j++){
        expectedData +=  Integer.toString((5000*i)+j);
      }
      
      assertEquals(expectedData, new String(Files.readAllBytes(path)));
      
    }
    
    
    FileUtils.deleteDirectory(new File(testDir));
  }
}
