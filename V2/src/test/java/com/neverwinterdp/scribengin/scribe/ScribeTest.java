package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.stream.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.streamconnector.StreamConnector;
import com.neverwinterdp.scribengin.streamconnector.StreamConnectorImpl;
import com.neverwinterdp.scribengin.task.TenPercentInvalidTask;

public class ScribeTest {
  @Test
  public void testScribe() throws Exception {

    StreamConnector stream = new StreamConnectorImpl(new UUIDSourceStream(), 
        new InMemorySinkStream(new DumbSinkPartitioner()), 
        new InMemorySinkStream(new DumbSinkPartitioner()), 
        new TenPercentInvalidTask());

    Scribe scribe = new ScribeImpl(stream);

    assertTrue(scribe.init());
    assertTrue(scribe.getStreamConnector().getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    assertEquals( ((UUIDSourceStream)stream.getSourceStream()).getData().size(),
        ((InMemorySinkStream)stream.getInvalidSink()).getData().size() +
        ((InMemorySinkStream)stream.getSinkStream()).getData().size());
    assertTrue(scribe.getStreamConnector().getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    assertEquals( ((UUIDSourceStream)stream.getSourceStream()).getData().size(),
        ((InMemorySinkStream)stream.getInvalidSink()).getData().size() +
        ((InMemorySinkStream)stream.getSinkStream()).getData().size());
    assertTrue(scribe.getStreamConnector().getTupleTracker().validateCounts());
  }
}
