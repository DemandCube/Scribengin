package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.sink.StdOutSinkStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class StdOutSinkStreamTest {
  
  @Test
  public void testStdOutSinkStream(){
    SinkStream sink = new StdOutSinkStream();
    
    int i=0;
    for(; i<10; i++){
      assertTrue(sink.append(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry("key",i,i))));
    }
    assertEquals(10L, sink.getBufferSize());
    
    assertTrue(sink.prepareCommit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.commit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.updateOffSet());
    assertEquals(0L, sink.getBufferSize());
  }
  
  
}
