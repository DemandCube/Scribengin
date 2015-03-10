package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;

public abstract class ScribeAbstract {
  protected ScribeState state;
  
  public abstract void process(Record record, DataflowTaskContext ctx) throws Exception;
  
  public ScribeState getState() { return this.state; }
  
  public void setState(ScribeState newState){
    this.state = newState;
  }
}