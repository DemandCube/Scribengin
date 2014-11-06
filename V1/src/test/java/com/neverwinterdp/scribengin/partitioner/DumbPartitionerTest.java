package com.neverwinterdp.scribengin.partitioner;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DumbPartitionerTest {
  @Test
  public void testDumbPartitionerRefresh(){
    DumbPartitioner d = new DumbPartitioner();
    assertEquals(null,d.getRefresh());
  }
  
  @Test
  public void testDumbPartitionerGetPartition(){
    DumbPartitioner d = new DumbPartitioner();
    assertEquals("",d.getPartition());
  }
}
