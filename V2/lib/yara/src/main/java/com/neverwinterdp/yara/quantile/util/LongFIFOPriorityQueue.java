package com.neverwinterdp.yara.quantile.util;

import java.util.LinkedList;

//TODO: implement a native long queue
public class LongFIFOPriorityQueue extends LinkedList<Long>{
  public void enqueue(Long val) {
    offer(val) ;
  }
  
  public Long dequeue() { return pop() ; }
  
  public long dequeueLong() { return pop() ; }
}
