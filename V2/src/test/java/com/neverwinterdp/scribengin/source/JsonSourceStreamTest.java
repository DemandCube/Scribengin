package com.neverwinterdp.scribengin.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.source.JsonSourceStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class JsonSourceStreamTest {
  @Test
  public void testJsonSourceStream(){
    SourceStream s = new JsonSourceStream();
    
    assertNotNull(s.getName());
    
    LinkedList<Tuple> list = new LinkedList<Tuple>();
    
    for(int i =0; i<10; i++){
      assertTrue(s.hasNext());
      Tuple t = s.readNext();
      assertEquals(Integer.toString(i),t.getKey());
      list.add(t);
    }
    assertTrue(list.size() == 10);
    assertTrue(s.prepareCommit());
    assertTrue(s.commit());
    assertTrue(s.updateOffSet());
    
    
    assertTrue(s.hasNext());
    Tuple t = s.readNext();
    assertTrue(s.prepareCommit());
    s.clearCommit();
    Tuple t2 = s.readNext();
    assertTrue(t.equals(t2));
    assertTrue(s.commit());
    assertTrue(s.updateOffSet());
    
  }
}
