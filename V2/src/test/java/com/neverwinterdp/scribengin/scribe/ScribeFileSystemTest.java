package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.FileSystemSinkStream;
import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.task.CopyTask;

public class ScribeFileSystemTest {
  static String testDir = "./"+UUID.randomUUID().toString()+"/"; 
  
  @AfterClass
  public static void tearDown() throws IOException{
    FileUtils.deleteDirectory(new File(testDir));
  }
  
  @Test
  public void testScribe() throws Exception {
    int bufferLimit = 5000;
    System.err.println("Working Directory = "+System.getProperty("user.dir")+testDir);
    
    Scribe scribe = new ScribeImpl(new SequentialIntSourceStream(), 
        new FileSystemSinkStream(testDir+"tmp", testDir+"commit/"), 
        new FileSystemSinkStream(testDir+"/invalid/tmp", testDir+"/invalid/commit/"), 
        new CopyTask(bufferLimit));

    

    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    
    ScribeFileTestHelper.assertValidDataWrittenToFile(bufferLimit, testDir);
    
  }

  
}
