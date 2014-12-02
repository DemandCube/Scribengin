package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.state.InMemoryScribeStateTracker;
import com.neverwinterdp.scribengin.scribe.state.ScribeState;
import com.neverwinterdp.scribengin.scribe.state.ScribeStateTracker;
import com.neverwinterdp.scribengin.stream.sink.FileSystemSinkStream;
import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.stream.streamdescriptor.FileSinkStreamDescriptor;
import com.neverwinterdp.scribengin.stream.streamdescriptor.OffsetStreamDescriptor;
import com.neverwinterdp.scribengin.task.CopyTask;

public class ScribeTotalFailureRestartTest {
  static String testDir; 
  static int bufferLimit = 1000;
  static int numIterations = 10;
  
  @AfterClass
  public static void tearDown() throws IOException{
    FileUtils.deleteDirectory(new File(testDir));
  }
  
  @Test
  public void testScribeTotalFailureRestartLoop() throws Exception{
    for(int i=0; i < numIterations; i++){
      testDir = "./"+UUID.randomUUID().toString()+"/";
      this.testScribeTotalFailureRestart();
      FileUtils.deleteDirectory(new File(testDir));
    }
  }
  
  //@Test
  public void testScribeTotalFailureRestart() throws Exception {
    ScribeStateTracker stateTracker = new InMemoryScribeStateTracker();
    
    assertEquals(ScribeState.UNINITIALIZED, stateTracker.getScribeState());
    
    //System.out.println("Working Directory = "+System.getProperty("user.dir")+testDir);
    
    SequentialIntSourceStream firstSource = new SequentialIntSourceStream();
    FileSystemSinkStream firstSink        = new FileSystemSinkStream(testDir+"tmp", testDir+"commit/");
    FileSystemSinkStream firstInvalidSink = new FileSystemSinkStream(testDir+"/invalid/tmp", testDir+"/invalid/commit/");
    
    Scribe scribe = new ScribeImpl(firstSource, 
        firstSink, 
        firstInvalidSink, 
        new CopyTask(bufferLimit),
        stateTracker);

    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    assertFalse(ScribeState.UNINITIALIZED.equals(stateTracker.getScribeState()));
    
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    //Call private method to kill thread in Scribe and stop it from running
    Method method = ((ScribeImpl)scribe).getClass().getDeclaredMethod("killScribeThread");
    method.setAccessible(true);
    ScribeState lastState = (ScribeState)method.invoke(scribe);
    
    //Validate data written so far
    Thread.sleep(100);
    ScribeFileTestHelper.assertValidDataWrittenToFile(bufferLimit, testDir);
    
    //Create new Scribe from old Scribe, set state as previous state
    Scribe recoveryScribe = new ScribeImpl(
        new SequentialIntSourceStream((OffsetStreamDescriptor)firstSource.getStreamDescriptor()), 
        new FileSystemSinkStream((FileSinkStreamDescriptor) firstSink.getStreamDescriptor()), 
        new FileSystemSinkStream((FileSinkStreamDescriptor) firstInvalidSink.getStreamDescriptor()), 
        new CopyTask(bufferLimit),
        new InMemoryScribeStateTracker(lastState));
    
    //System.err.println("STATE IN TEST BEFORE RECOVERY: "+lastState.toString());
    
    //Call recovery
    assertTrue(recoveryScribe.recover());
    
    //Business as usual
    assertTrue(recoveryScribe.init());
    assertTrue(recoveryScribe.getTupleTracker().validateCounts());
    
    recoveryScribe.start();
    Thread.sleep(500);
    assertFalse(ScribeState.UNINITIALIZED.equals(stateTracker.getScribeState()));
    recoveryScribe.stop();
    Thread.sleep(500);
    assertTrue(recoveryScribe.getTupleTracker().getWritten() > 0);
    //System.err.println(recoveryScribe.getTupleTracker());
    assertTrue(recoveryScribe.getTupleTracker().validateCounts());
    ScribeFileTestHelper.assertValidDataWrittenToFile(bufferLimit, testDir);
    
  }
}
