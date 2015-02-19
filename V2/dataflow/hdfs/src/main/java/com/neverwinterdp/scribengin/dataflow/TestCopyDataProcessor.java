package com.neverwinterdp.scribengin.dataflow;

import java.util.Random;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;

public class TestCopyDataProcessor extends ScribeAbstract {
  private int count = 0;
  private Random random = new Random();
  
  
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    if(random.nextDouble() < 0.8) {
      ctx.write("default",record);
      //System.out.println("Write default");
    } else {
      ctx.write("invalid", record);
      //System.out.println("Write invalid");
    }
    count++ ;
    if(count == 100) {
      System.out.println("Copy  100 records, commit") ;
      ctx.commit();
      count = 0;
    }
  }
}