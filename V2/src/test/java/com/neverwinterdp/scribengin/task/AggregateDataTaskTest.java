package com.neverwinterdp.scribengin.task;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.StdOutSinkStream;
import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;

public class AggregateDataTaskTest {
  
  @Test
  public void testAggregateDataTask(){
    Scribe scribe = new ScribeImpl(new JsonSourceStream(), 
        new StdOutSinkStream(), 
        new InMemorySinkStream(),
        new AggregateDataTask(100));
  
  
    for(int i=0; i<10; i++){
      scribe.processNext();
    }
    
    assertEquals(1000L, ((InMemorySinkStream)scribe.getInvalidSink()).getData().size());
    assertEquals(10L, ((StdOutSinkStream)scribe.getSinkStream()).getNumTuplesOutput());
    
  }
}
