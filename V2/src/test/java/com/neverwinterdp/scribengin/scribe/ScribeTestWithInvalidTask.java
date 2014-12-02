package com.neverwinterdp.scribengin.scribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.scribe.state.ScribeState;
import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.stream.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.task.TenPercentInvalidTask;

public class ScribeTestWithInvalidTask {
  
  @Test
  public void testScribeWithInvalidTaskl() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException{
    Scribe scribe = new ScribeImpl(new UUIDSourceStream(), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new TenPercentInvalidTask());
    
    scribe.setState(ScribeState.INIT);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    for(int i=0; i<50; i++){
      //Processes 10 records at a time
      scribe.processNext();
    }
    
    System.err.println(scribe.getTupleTracker());
    assertTrue(scribe.getTupleTracker().validateCounts());
    assertEquals(450,((InMemorySinkStream)scribe.getSinkStream()).getData().size());
    assertEquals(50,((InMemorySinkStream)scribe.getInvalidSink()).getData().size());
  }
}
