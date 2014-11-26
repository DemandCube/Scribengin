package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.RandomFailureSinkStream;
import com.neverwinterdp.scribengin.stream.source.RandomFailureSourceStream;
import com.neverwinterdp.scribengin.task.TenPercentInvalidTask;

public class ScribeWithFailuresTest {
  @Test
  public void testScribeWithFailures() throws Exception {

    Scribe scribe = new ScribeImpl(new RandomFailureSourceStream(10), 
        new RandomFailureSinkStream(10), 
        new RandomFailureSinkStream(10), 
        new TenPercentInvalidTask());

    

    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(4500);
    scribe.stop();
    Thread.sleep(100);
    
    System.out.println(scribe.getTupleTracker());
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    System.out.println(scribe.getTupleTracker());
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
  }
}
