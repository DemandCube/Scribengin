package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;

public class SimpleScribe implements ScribeInterface {
  private int count = 0;
  
  @Override
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    ctx.append(record);
    count++ ;
    if(count == 100) {
      ctx.commit();
      count = 0;
    }
  }

}
