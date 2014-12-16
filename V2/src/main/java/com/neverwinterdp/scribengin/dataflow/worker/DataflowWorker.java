package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;

public class DataflowWorker {
  private Logger logger = LoggerFactory.getLogger(DataflowWorker.class);

  @Inject
  private DataflowContainer container;
 
  private DataflowDescriptor dataflowDescriptor;
  
  private List<DataflowTaskExecutor> taskExecutors;
  
  public DataflowContainer getDataflowContainer() { return container; }

  public DataflowDescriptor getDataflowDescriptor() { return dataflowDescriptor; }

  @Inject
  public void onInit() throws Exception {
    DataflowRegistry dataflowRegistry = container.getDataflowRegistry();
    dataflowDescriptor = dataflowRegistry.getDataflowDescriptor();
    int numOfExecutors = dataflowDescriptor.getNumberOfExecutorsPerWorker();
    taskExecutors = new ArrayList<DataflowTaskExecutor>();
    for(int i = 0; i < numOfExecutors; i++) {
      DataflowTaskExecutor executor = new DataflowTaskExecutor(container);
      executor.start();
      taskExecutors.add(executor);
    }
  }
  
  public void onDestroy() {
    System.out.println("onDestroy() DataflowWorker............");
  }
  
  public DataflowWorkerDescriptor getDescriptor() {
    return null ;
  }
  
  public DataflowTaskExecutor[] getExecutors() {
    return null;
  }
}