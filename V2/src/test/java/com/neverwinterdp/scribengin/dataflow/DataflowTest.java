package com.neverwinterdp.scribengin.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.streamcoordinator.DumbStreamCoordinator;

public class DataflowTest {
  @Test
  public void testDataFlow() throws Exception {
    
    Dataflow d = new DataflowImpl("Test", new DumbStreamCoordinator(5));
    
    d.initScribes();
    d.start();
    Thread.sleep(1500);
    d.pause();
    //Gives threads a chance to end and catch up
    Thread.sleep(100);

    Scribe[] scribes = d.getScribes();
    for(Scribe s: scribes){
      assertEquals( ((UUIDSourceStream)s.getSourceStream()).getData().size(),
          ((InMemorySinkStream)s.getInvalidSink()).getData().size() +
          ((InMemorySinkStream)s.getSinkStream()).getData().size());
      //System.err.println(s.getStreamConnector().getTupleTracker());
      assertTrue(s.getTupleTracker().getWritten() > 0);
      assertTrue(s.getTupleTracker().validateCounts());
    }

    d.start();
    Thread.sleep(1500);
    d.pause();
    //Gives threads a chance to end and catch up
    Thread.sleep(100);

    for(Scribe s: scribes){
      assertEquals( ((UUIDSourceStream)s.getSourceStream()).getData().size(),
          ((InMemorySinkStream)s.getInvalidSink()).getData().size() +
          ((InMemorySinkStream)s.getSinkStream()).getData().size());
      //System.err.println(s.getStreamConnector().getTupleTracker());
      assertTrue(s.getTupleTracker().getWritten() > 0);
      assertTrue(s.getTupleTracker().validateCounts());
    }


    d.stop();
    assertEquals("Test", d.getName());
    
  }
}
