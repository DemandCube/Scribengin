package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import org.junit.Test;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class InMemorySinkStreamTest {
  
  @Test
  public void testInMemorySinkStream(){
    SinkStream sink = new InMemorySinkStream(new DumbSinkPartitioner());
    
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
    
    //Now we'll do a commit and roll it back
    LinkedList<Tuple> originalData = new LinkedList<Tuple>();
    originalData.addAll(((InMemorySinkStream)sink).getData());
    for(; i<20; i++){
      assertTrue(sink.append(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
    }
    assertEquals(10L, sink.getBufferSize());
    
    assertTrue(sink.prepareCommit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.commit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.rollBack());
    assertEquals(0L, sink.getBufferSize());
    
    assertEquals(originalData.size(), ((InMemorySinkStream)sink).getData().size());
    for(int j=0; j < originalData.size(); j++){
      assertTrue(originalData.get(j).equals(  ((InMemorySinkStream)sink).getData().get(j)  ));
    }
  }
}
