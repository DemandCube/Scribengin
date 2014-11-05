package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.source.DataSourceStream;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.source.DataSource;

public interface DataFlowTask {
  public int getId() ;
  public DataSource getDataSource();
  public DataSourceStream getAssignedDataSourceStream();
  public Sink getSink();
}