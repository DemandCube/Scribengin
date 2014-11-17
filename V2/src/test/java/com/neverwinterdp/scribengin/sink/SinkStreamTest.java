package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.scribe.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class SinkStreamTest {
  @Test
  public void testInMemorySinkStream(){
    SinkStream sink = new InMemorySinkStream(new DumbSinkPartitioner());
    assertTrue(sink.openStream());

    for(int i=0; i<10; i++){
      assertNotNull(sink.writeTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry())));
    }

    assertTrue(sink.closeStream());
  }
}
