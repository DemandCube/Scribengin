package com.neverwinterdp.scribengin.stream.streamdescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class JsonSourceStreamDescriptorTest {
  @Test
  public void testJsonSourceStreamDescriptor(){
    SourceStream source = new JsonSourceStream();
    
    assertNotNull(source.getName());
    
    LinkedList<Tuple> list = new LinkedList<Tuple>();
    
    for(int i =0; i<10; i++){
      assertTrue(source.hasNext());
      Tuple t = source.readNext();
      assertEquals(Integer.toString(i),t.getKey());
      list.add(t);
    }
    assertTrue(list.size() == 10);
    assertTrue(source.prepareCommit());
    assertTrue(source.commit());
    assertTrue(source.completeCommit());
    
    //Create a new source from the descriptor
    JsonSourceStreamDescriptor sDesc = (JsonSourceStreamDescriptor) source.getStreamDescriptor();
    SourceStream secondSource = new JsonSourceStream(sDesc);
    assertEquals(source.getName(), sDesc.getName());
    assertEquals(source.getName(), secondSource.getName());
    
    //Make sure secondSource picks up where the old source left off
    for(int i =10; i<20; i++){
      assertTrue(secondSource.hasNext());
      Tuple t = secondSource.readNext();
      assertEquals(Integer.toString(i),t.getKey());
      list.add(t);
    }
    
    assertTrue(list.size() == 20);
    assertTrue(secondSource.prepareCommit());
    assertTrue(secondSource.commit());
    assertTrue(secondSource.completeCommit());
  }
}