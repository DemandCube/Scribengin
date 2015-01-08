package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.Record;

public interface DataProcessor {
  public void process(Record record, DataflowTaskContext ctx) throws Exception;
}
