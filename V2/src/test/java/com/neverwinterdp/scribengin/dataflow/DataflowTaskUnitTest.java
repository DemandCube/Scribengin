package com.neverwinterdp.scribengin.dataflow;

import org.junit.Test;

public class DataflowTaskUnitTest {
  
  @Test
  public void test() throws Exception {
    DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
    DataflowTask task = new DataflowTask();
    task.onInit(descriptor);
    task.suspend();
  }
}
