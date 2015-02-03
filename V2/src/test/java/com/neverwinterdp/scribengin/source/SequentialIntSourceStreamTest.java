package com.neverwinterdp.scribengin.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;

import org.junit.Test;

import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.stream.source.SourceStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class SequentialIntSourceStreamTest {
  @Test
  public void testSequentialIntSourceStream() throws IOException{
    SourceStream s = new SequentialIntSourceStream();
    
    assertNotNull(s.getName());
    
    LinkedList<Tuple> list = new LinkedList<Tuple>();
    
    for(int i = 1; i<= 10; i++){
      assertTrue(s.hasNext());
      Tuple t = s.readNext();
      //System.err.println(new String(t.getData()));
      assertEquals(Integer.toString(i),  new String(t.getData()));
      list.add(t);
    }
    
    assertTrue(list.size() == 10);
    assertTrue(s.prepareCommit());
    assertTrue(s.commit());
    assertTrue(s.completeCommit());
    //At 10
    
    
    assertTrue(s.hasNext());
    Tuple t = s.readNext();  //11
    assertTrue(s.prepareCommit());
    s.clearBuffer();  //10
    
    Tuple t2 = s.readNext(); // @11
    assertTrue(t.equals(t2));
    assertTrue(s.prepareCommit());
    assertTrue(s.commit());
    assertTrue(s.completeCommit());
    
    
    //Test rollback
    for(int i = 12; i <= 21; i++){
      assertTrue(s.hasNext());
      Tuple t1 = s.readNext();
      assertEquals(Integer.toString(i),  new String(t1.getData()));
      list.add(t1);
    }
    
    //Prepare for commit, then call rollback
    assertTrue(s.prepareCommit());
    assertTrue(s.commit());
    assertTrue(s.rollBack());
    
    //Make sure we haven't lost our place
    for(int i = 12; i<21; i++){
      assertTrue(s.hasNext());
      Tuple t1 = s.readNext();
      assertEquals(Integer.toString(i),  new String(t1.getData()));
      list.add(t1);
    }
  }
  
}
