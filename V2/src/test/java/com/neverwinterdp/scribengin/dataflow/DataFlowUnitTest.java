package com.neverwinterdp.scribengin.dataflow;


import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.ri.DataFlowImpl;

public class DataFlowUnitTest {
  @Test
  public void testDataFlow() throws Exception {
    DataFlow dataflow = new DataFlowImpl("RI") ;
    test(dataflow);
  }
  
  private void test(DataFlow dataFlow) throws Exception {
    for(int i = 0; i < 5; i++) {
      DataStream stream = dataFlow.newDataStream() ;
      DataStreamWriter streamWriter = stream.getWriter() ;
      for(int j = 0; j  < 10; j++) {
        streamWriter.append(createRecord("key-" + i + "-" + j, 32));
      }
      streamWriter.close();
    }
    DataStream[] streams = dataFlow.getDataStreams() ;
    Assert.assertEquals(5, streams.length);
    
    DataFlowReader reader = dataFlow.getReader() ;
    int recordCount = 0 ;
    Record record = null ;
    while((record = reader.next()) != null) {
      recordCount++ ;
    }
    Assert.assertEquals(50, recordCount);
    dataFlow.close();
  }
  
  private Record createRecord(String key, int size) {
    byte[] data = new byte[size];
    Record record = new Record(key, data) ;
    return record;
  }
}