package com.neverwinterdp.scribengin.task;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.StdOutSinkStream;
import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;
import com.neverwinterdp.scribengin.streamconnector.StreamConnector;
import com.neverwinterdp.scribengin.streamconnector.StreamConnectorImpl;

public class JsonToCsvConverterTaskTest {
  
  @Test
  public void testJsonToCsvConverterTask(){
    StreamConnector streamConn = new StreamConnectorImpl(new JsonSourceStream(), 
        new StdOutSinkStream(), 
        new StdOutSinkStream(), 
        new JsonToCSVConverterTask());
  
  
    for(int i=0; i<50; i++){
      //Processes 10 records at a time
      streamConn.processNext();
    }
    
    assertEquals(0L, ((StdOutSinkStream)streamConn.getInvalidSink()).getNumTuplesOutput());
    assertEquals(500L, ((StdOutSinkStream)streamConn.getSinkStream()).getNumTuplesOutput());
    
  }
}
