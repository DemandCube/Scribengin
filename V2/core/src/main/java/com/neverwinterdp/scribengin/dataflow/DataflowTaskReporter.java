package com.neverwinterdp.scribengin.dataflow;

import com.google.inject.Singleton;

@Singleton
public class DataflowTaskReporter {
  public void create(DataflowContainer container, DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws Exception {
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    dRegistry.create(descriptor, report);
  }
  
  public void report(DataflowContainer container, DataflowTaskDescriptor descriptor, DataflowTaskReport report) throws Exception {
    DataflowRegistry dRegistry = container.getDataflowRegistry();
    dRegistry.dataflowTaskReport(descriptor, report);
  }
}