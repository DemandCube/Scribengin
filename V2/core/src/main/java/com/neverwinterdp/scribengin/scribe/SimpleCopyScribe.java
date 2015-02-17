package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;

public class SimpleCopyScribe extends ScribeAbstract {
  private int count = 0;
  
  public SimpleCopyScribe(){
    this.setState(ScribeState.INIT);
  }
  
  public void process(Record record, DataflowTaskContext ctx) throws Exception {ctx.append(record);
    ctx.append(record);
    count++ ;
    if(count == 100) {
      setState(ScribeState.COMMITTING);
      ctx.commit();
      count = 0;
    } else{
      setState(ScribeState.BUFFERING);
    }
  }
}
