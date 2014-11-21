package com.neverwinterdp.scribengin.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;
import com.neverwinterdp.scribengin.streamconnector.StreamConnector;
import com.neverwinterdp.scribengin.streamconnector.StreamConnectorImpl;

public class DoubleTheTuplesTaskTest {
  
  @Test
  public void testDoubleTheTuplesTask(){
    StreamConnector streamConn = new StreamConnectorImpl(new JsonSourceStream(), 
        new InMemorySinkStream(), 
        new InMemorySinkStream(),
        new DoubleTheTuplesTask());
  
  
    for(int i=0; i<1; i++){
      //Processes 10 records at a time
      streamConn.processNext();
    }
    
    assertEquals(0L, ((InMemorySinkStream)streamConn.getInvalidSink()).getData().size());
    assertEquals(10L, ((InMemorySinkStream)streamConn.getSinkStream()).getData().size());
    
    
    for(int i=0; i<((InMemorySinkStream)streamConn.getSinkStream()).getData().size(); i++){
      if(i % 2 == 1){
        //Make sure every other key starts with "Generated"
        assertTrue(((InMemorySinkStream)streamConn.getSinkStream()).getData().get(i).getKey().startsWith("Generated"));
      }
      else{
      //Make sure the rest of teh keys are numeric
        assertTrue(Character.isDigit(((InMemorySinkStream)streamConn.getSinkStream()).getData().get(i).getKey().charAt(0)));
      }
    }
  }
}
