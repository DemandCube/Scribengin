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
    logger.info("Start onInit()");
    DataflowRegistry dataflowRegistry = container.getDataflowRegistry();
    dataflowDescriptor = dataflowRegistry.getDataflowDescriptor();
    int numOfExecutors = dataflowDescriptor.getNumberOfExecutorsPerWorker();
    taskExecutors = new ArrayList<DataflowTaskExecutor>();
    for(int i = 0; i < numOfExecutors; i++) {
      DataflowTaskExecutor executor = new DataflowTaskExecutor(container);
      executor.start();
      taskExecutors.add(executor);
    }
    logger.info("Finish onInit()");
  }
  
  public void shutdown() {
    logger.info("Start shutdown()");
    //TODO: Use dataflowRegistry to mark the status and notify
    logger.info("Finish shutdown()");
  }
  
  public boolean isAlive() {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) return true;
    }
    return false;
  }
  
  public void waitForTermination(long checkPeriod) throws InterruptedException {
    while(isAlive()) {
      Thread.sleep(1000);
    }
  }
  
  public DataflowWorkerDescriptor getDescriptor() {
    return null ;
  }
  
  public List<DataflowTaskExecutor> getExecutors() { return taskExecutors; }
}