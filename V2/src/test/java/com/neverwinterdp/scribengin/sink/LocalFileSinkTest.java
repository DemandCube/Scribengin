package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.LocalFileSinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class LocalFileSinkTest {
  
  @Test
  public void testStdOutSinkStream() throws IOException{
    String testDir = "./tmp/";
    SinkStream sink = new LocalFileSinkStream(testDir);
    
    int i=0;
    for(; i<10; i++){
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry("key",i,i))));
    }
    assertEquals(10L, sink.getBufferSize());
    
    assertTrue(sink.prepareCommit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.commit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.completeCommit());
    assertEquals(0L, sink.getBufferSize());
    
    for(i=0; i< 10; i++){
      File f = new File(testDir+Integer.toString(i));
      assertTrue(f.exists());
    }
    
    
    for(i=10; i<20; i++){
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry("key",i,i))));
    }
    assertEquals(10L, sink.getBufferSize());
    
    assertTrue(sink.prepareCommit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.commit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.rollBack());
    assertEquals(0L, sink.getBufferSize());
    
    
    for(i=0; i< 10; i++){
      File f = new File(testDir+Integer.toString(i));
      assertTrue(f.exists());
    }
    
    for(; i< 20; i++){
      File f = new File(testDir+Integer.toString(i));
      assertFalse(f.exists());
    }
    
    FileUtils.deleteDirectory(new File(testDir));
  }
}
