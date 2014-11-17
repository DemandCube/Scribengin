package com.neverwinterdp.scribengin.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.task.DumbTask;

public class StreamTest {
  
  @Test
  public void testStreamImpl(){
    Stream stream = new StreamImpl(new UUIDSourceStream(), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new DumbTask());
    
    assertTrue(stream.initStreams());
    
    for(int i=0; i<50; i++){
      stream.processNext();
    }
    
    assertEquals(45,((InMemorySinkStream)stream.getSinkStream()).getData().size());
    assertEquals(5,((InMemorySinkStream)stream.getInvalidSink()).getData().size());
    
    assertTrue(stream.verifyDataInSink());
    assertTrue(stream.closeStreams());
    
  }
}
