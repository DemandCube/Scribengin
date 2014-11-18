package com.neverwinterdp.scribengin.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Test;

import com.neverwinterdp.scribengin.commitlog.CommitLog;
import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.task.DumbTask;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class StreamTest {
  
  @Test
  public void testStreamImpl() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException{
    Stream stream = new StreamImpl(new UUIDSourceStream(), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new DumbTask());
    
    assertTrue(stream.initStreams());
    
    for(int i=0; i<50; i++){
      stream.processNext();
    }
    
    assertEquals(45,((InMemorySinkStream)stream.getSinkStream()).getData().size());
    assertEquals(5,((InMemorySinkStream)stream.getInvalidSink()).getData().size());
    
    //Make sure data from source matches what's in sink
    assertTrue(stream.verifyDataInSink());
    
    
    //Write bad data, ensure verification fails
    CommitLog c = ((StreamImpl)stream).getCommitLog();
    stream.getSinkStream().writeTuple(new Tuple("key","blah".getBytes(), new CommitLogEntry()));
    c.addNextEntry(stream.getSourceStream().readNext().getCommitLogEntry());
    
    
    Method m = StreamImpl.class.getDeclaredMethod("setCommitLog",CommitLog.class);
    m.setAccessible(true);
    m.invoke(stream, c);
    
    assertFalse(stream.verifyDataInSink());
    
    
    assertTrue(stream.fixDataInSink());
    assertTrue(stream.verifyDataInSink());
    
    
    assertTrue(stream.closeStreams());
  }
}
