package com.neverwinterdp.scribengin.task;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.stream.sink.StdOutSinkStream;
import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;

public class JsonToCsvConverterTaskTest {
  
  @Test
  public void testJsonToCsvConverterTask(){
    Scribe scribe = new ScribeImpl(new JsonSourceStream(), 
        new StdOutSinkStream(), 
        new StdOutSinkStream(), 
        new JsonToCSVConverterTask());
  
  
    for(int i=0; i<50; i++){
      //Processes 10 records at a time
      scribe.processNext();
    }
    
    assertEquals(0L, ((StdOutSinkStream)scribe.getInvalidSink()).getNumTuplesOutput());
    assertEquals(500L, ((StdOutSinkStream)scribe.getSinkStream()).getNumTuplesOutput());
    
  }
}
