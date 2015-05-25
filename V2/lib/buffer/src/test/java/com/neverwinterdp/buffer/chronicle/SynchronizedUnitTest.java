package com.neverwinterdp.buffer.chronicle;

import java.util.Random;

import org.junit.Test;

public class SynchronizedUnitTest {
  @Test
  public void test() {
    int CALL = 10000000;
    
    long start = System.currentTimeMillis() ;
    for(int i = 0; i < CALL ; i++) normalCall() ;
    System.out.println("Normal call in " + (System.currentTimeMillis() - start) + "ms") ;
    
    start = System.currentTimeMillis() ;
    for(int i = 0; i < CALL ; i++) synchronizedCall() ;
    System.out.println("Synchronize call in " + (System.currentTimeMillis() - start) + "ms") ;
  }
  
  synchronized void synchronizedCall() {
    new Random().nextInt() ;
  }
  
  synchronized void normalCall() {
    new Random().nextInt() ;
  }
}
