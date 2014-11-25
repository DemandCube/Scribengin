package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.RandomFailureSinkStream;
import com.neverwinterdp.scribengin.stream.source.RandomFailureSourceStream;
import com.neverwinterdp.scribengin.streamconnector.StreamConnector;
import com.neverwinterdp.scribengin.streamconnector.StreamConnectorImpl;
import com.neverwinterdp.scribengin.task.TenPercentInvalidTask;

public class ScribeWithFailuresTest {
  @Test
  public void testScribeWithFailures() throws Exception {

    StreamConnector stream = new StreamConnectorImpl(new RandomFailureSourceStream(), 
        new RandomFailureSinkStream(25), 
        new RandomFailureSinkStream(25), 
        new TenPercentInvalidTask());

    Scribe scribe = new ScribeImpl(stream);

    assertTrue(scribe.init());
    assertTrue(scribe.getStreamConnector().getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(4500);
    scribe.stop();
    Thread.sleep(100);
    
    System.out.println(scribe.getStreamConnector().getTupleTracker());
    assertTrue(scribe.getStreamConnector().getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getStreamConnector().getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    System.out.println(scribe.getStreamConnector().getTupleTracker());
    assertTrue(scribe.getStreamConnector().getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getStreamConnector().getTupleTracker().validateCounts());
  }
}
