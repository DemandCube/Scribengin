package com.neverwinterdp.scribengin.sink.ri;


import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.scribengin.sink.ri.SinkImpl;

public class SinkUnitTest {
  @Test
  public void testSink() throws Exception {
    Sink dataflow = new SinkImpl("RI") ;
    test(dataflow);
  }
  
  private void test(Sink dataFlow) throws Exception {
    for(int i = 0; i < 5; i++) {
      SinkStream stream = dataFlow.newSinkStream() ;
      SinkStreamWriter streamWriter = stream.getWriter() ;
      for(int j = 0; j  < 10; j++) {
        streamWriter.append(createRecord("key-" + i + "-" + j, 32));
      }
      streamWriter.close();
    }
    SinkStream[] streams = dataFlow.getDataStreams() ;
    Assert.assertEquals(5, streams.length);
    
    dataFlow.close();
  }
  
  private Record createRecord(String key, int size) {
    byte[] data = new byte[size];
    Record record = new Record(key, data) ;
    return record;
  }
}