package com.neverwinterdp.scribengin.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import org.junit.Test;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class SourceStreamTest {
  @Test
  public void testUUIDSourceStream(){
    SourceStream s = new UUIDSourceStream();
    
    LinkedList<Tuple> list = new LinkedList<Tuple>();
    
    assertTrue(s.openStream());
    
    for(int i =0; i<10; i++){
      assertTrue(s.hasNext());
      Tuple t = s.readNext();
      assertEquals(Integer.toString(i+1),t.getKey());
      list.add(t);
    }
    assertTrue(list.size() == 10);
    
    assertTrue(s.closeStream());
  }
}
