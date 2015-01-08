package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;

public class CopyDataProcessor implements DataProcessor {
  private int count = 0;
  
  @Override
  public void process(Record record, DataflowTaskContext ctx) throws Exception {
    ctx.write(record);
    count++ ;
    if(count == 100) {
      ctx.commit();
      count = 0;
    }
  }

}
