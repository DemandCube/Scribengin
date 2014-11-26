package com.neverwinterdp.scribengin.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;

public class DoubleTheTuplesTaskTest {
  
  @Test
  public void testDoubleTheTuplesTask(){
    Scribe scribe = new ScribeImpl(new JsonSourceStream(), 
        new InMemorySinkStream(), 
        new InMemorySinkStream(),
        new DoubleTheTuplesTask());
  
  
    for(int i=0; i<1; i++){
      //Processes 10 records at a time
      scribe.processNext();
    }
    
    assertEquals(0L, ((InMemorySinkStream)scribe.getInvalidSink()).getData().size());
    assertEquals(10L, ((InMemorySinkStream)scribe.getSinkStream()).getData().size());
    
    
    for(int i=0; i<((InMemorySinkStream)scribe.getSinkStream()).getData().size(); i++){
      if(i % 2 == 1){
        //Make sure every other key starts with "Generated"
        assertTrue(((InMemorySinkStream)scribe.getSinkStream()).getData().get(i).getKey().startsWith("Generated"));
      }
      else{
      //Make sure the rest of teh keys are numeric
        assertTrue(Character.isDigit(((InMemorySinkStream)scribe.getSinkStream()).getData().get(i).getKey().charAt(0)));
      }
    }
  }
}
