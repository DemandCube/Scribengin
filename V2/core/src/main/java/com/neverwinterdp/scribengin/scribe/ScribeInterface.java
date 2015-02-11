package com.neverwinterdp.scribengin.scribe;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;

public interface ScribeInterface {
  public void process(Record record, DataflowTaskContext ctx) throws Exception;
}
