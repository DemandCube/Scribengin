package com.neverwinterdp.scribengin.task;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.StdOutSinkStream;
import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;
import com.neverwinterdp.scribengin.streamconnector.StreamConnector;
import com.neverwinterdp.scribengin.streamconnector.StreamConnectorImpl;

public class AggregateDataTaskTest {
  
  @Test
  public void testAggregateDataTask(){
    StreamConnector streamConn = new StreamConnectorImpl(new JsonSourceStream(), 
        new StdOutSinkStream(), 
        new InMemorySinkStream(),
        new AggregateDataTask(100));
  
  
    for(int i=0; i<10; i++){
      streamConn.processNext();
    }
    
    assertEquals(1000L, ((InMemorySinkStream)streamConn.getInvalidSink()).getData().size());
    assertEquals(10L, ((StdOutSinkStream)streamConn.getSinkStream()).getNumTuplesOutput());
    
  }
}
