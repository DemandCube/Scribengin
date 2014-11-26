package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.stream.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.task.TenPercentInvalidTask;

public class ScribeTest {
  @Test
  public void testScribe() throws Exception {

    Scribe scribe = new ScribeImpl(new UUIDSourceStream(), 
        new InMemorySinkStream(new DumbSinkPartitioner()), 
        new InMemorySinkStream(new DumbSinkPartitioner()), 
        new TenPercentInvalidTask());

    

    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    assertEquals( ((UUIDSourceStream)scribe.getSourceStream()).getData().size(),
        ((InMemorySinkStream)scribe.getInvalidSink()).getData().size() +
        ((InMemorySinkStream)scribe.getSinkStream()).getData().size());
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    assertEquals( ((UUIDSourceStream)scribe.getSourceStream()).getData().size(),
        ((InMemorySinkStream)scribe.getInvalidSink()).getData().size() +
        ((InMemorySinkStream)scribe.getSinkStream()).getData().size());
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
  }
}
