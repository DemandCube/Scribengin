package com.neverwinterdp.scribengin.streamconnector;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.sink.InMemorySinkStream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.DumbSinkPartitioner;
import com.neverwinterdp.scribengin.stream.source.UUIDSourceStream;
import com.neverwinterdp.scribengin.task.TenPercentInvalidTask;

public class StreamConnectorTest {
  
  @Test
  public void testStreamConnectorImpl() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException{
    StreamConnector streamConn = new StreamConnectorImpl(new UUIDSourceStream(), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new InMemorySinkStream(new DumbSinkPartitioner()), 
                                      new TenPercentInvalidTask());
    
    for(int i=0; i<50; i++){
      //Processes 10 records at a time
      streamConn.processNext();
    }
    
    assertEquals(450,((InMemorySinkStream)streamConn.getSinkStream()).getData().size());
    assertEquals(50,((InMemorySinkStream)streamConn.getInvalidSink()).getData().size());
    
    /*
    
    //Write bad data, ensure verification fails
    CommitLog c = ((StreamConnectorImpl)stream).getCommitLog();
    stream.getSinkStream().writeTuple(new Tuple("key","blah".getBytes(), new CommitLogEntry()));
    c.addNextEntry(stream.getSourceStream().readNext().getCommitLogEntry());
    
    
    Method m = StreamConnectorImpl.class.getDeclaredMethod("setCommitLog",CommitLog.class);
    m.setAccessible(true);
    m.invoke(stream, c);
    
    //Make sure sink is messed up
    assertFalse(stream.verifyDataInSink());
    
    //Fix data in sink
    assertTrue(stream.fixDataInSink());
    //Verify fix (fixDataInSink() should already call verifyDataInSink(), but just being verbose)
    assertTrue(stream.verifyDataInSink());
    
    
    */
  }
}
