package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.stream.Stream;
import com.neverwinterdp.scribengin.stream.StreamImpl;
import com.neverwinterdp.scribengin.task.DumbTask;

public class ScribeTest {
  @Test
  public void testScribe() throws Exception {

    Stream stream = new StreamImpl(new UUIDSourceStream(), 
        new InMemorySinkStream(new DumbSinkPartitioner()), 
        new InMemorySinkStream(new DumbSinkPartitioner()), 
        new DumbTask());

    Scribe scribe = new ScribeImpl(stream);

    assertTrue(scribe.init());

    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    assertEquals( ((UUIDSourceStream)stream.getSourceStream()).getNumTuples(),
        ((InMemorySinkStream)stream.getInvalidSink()).getData().size() +
        ((InMemorySinkStream)stream.getSinkStream()).getData().size());

    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    assertEquals( ((UUIDSourceStream)stream.getSourceStream()).getNumTuples(),
        ((InMemorySinkStream)stream.getInvalidSink()).getData().size() +
        ((InMemorySinkStream)stream.getSinkStream()).getData().size());

    assertTrue(scribe.close());
  }
}
