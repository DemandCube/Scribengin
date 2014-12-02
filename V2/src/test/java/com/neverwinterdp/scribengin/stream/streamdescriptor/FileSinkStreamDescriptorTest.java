package com.neverwinterdp.scribengin.stream.streamdescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeFileTestHelper;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.stream.sink.FileSystemSinkStream;
import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.task.CopyTask;

public class FileSinkStreamDescriptorTest {
  static String testDir = "./"+UUID.randomUUID().toString()+"/"; 
  
  @AfterClass
  public static void tearDown() throws IOException{
    FileUtils.deleteDirectory(new File(testDir));
  }
  
  @Test
  public void testFileSinkStreamDescriptor() throws InterruptedException, IOException{
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
    
    long firstScribeTupleCount = scribe.getTupleTracker().getWritten();
    
    //Create the second scribe from the descriptors of the first scribe
    Scribe secondScribe = new ScribeImpl(
        new SequentialIntSourceStream((OffsetStreamDescriptor)scribe.getSourceStream().getStreamDescriptor()), 
        new FileSystemSinkStream((FileSinkStreamDescriptor) scribe.getSinkStream().getStreamDescriptor()), 
        new FileSystemSinkStream((FileSinkStreamDescriptor) scribe.getInvalidSink().getStreamDescriptor()), 
        new CopyTask(bufferLimit));
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    assertTrue(secondScribe.init());
    assertTrue(secondScribe.getTupleTracker().validateCounts());
    
    secondScribe.start();
    Thread.sleep(1500);
    secondScribe.stop();
    Thread.sleep(100);
    
    assertEquals(firstScribeTupleCount, scribe.getTupleTracker().getWritten());
    assertTrue(secondScribe.getTupleTracker().getWritten() > 0);
    assertTrue(secondScribe.getTupleTracker().validateCounts());
    
    
    ScribeFileTestHelper.assertValidDataWrittenToFile(bufferLimit, testDir);
  }
}
