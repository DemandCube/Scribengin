package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;

@Singleton
@JmxBean("role=dataflow-worker, type=DataflowTaskExecutorService, dataflowName=DataflowTaskExecutorService")
public class DataflowTaskExecutorService {
  private Logger logger = LoggerFactory.getLogger(DataflowTaskExecutorService.class);

  @Inject
  private DataflowContainer container;
  private DataflowTaskWorkerEventListenter dataflowTaskEventListener ;
  private DataflowDescriptor dataflowDescriptor;
  private List<DataflowTaskExecutor> taskExecutors;
  
  @Inject
  public void onInit(DataflowRegistry dflRegistry) throws Exception {
    logger.info("Start onInit()");
    dataflowTaskEventListener = new DataflowTaskWorkerEventListenter(dflRegistry);
    dataflowDescriptor = dflRegistry.getDataflowDescriptor();
    int numOfExecutors = dataflowDescriptor.getNumberOfExecutorsPerWorker();
    taskExecutors = new ArrayList<DataflowTaskExecutor>();
    for(int i = 0; i < numOfExecutors; i++) {
      DataflowTaskExecutorDescriptor descriptor = new DataflowTaskExecutorDescriptor ("executor-" + i);
      DataflowTaskExecutor executor = new DataflowTaskExecutor(descriptor, container);
      taskExecutors.add(executor);
    }
    start();
    logger.info("Finish onInit()");
  }
  
  public void start() throws Exception {
    logger.info("Start start()");
    for(int i = 0; i < taskExecutors.size(); i++) {
      DataflowTaskExecutor executor = taskExecutors.get(i);
      executor.start();
    }
    logger.info("Finish start()");
  }
  
  
  
  public void shutdown() throws Exception {
    logger.info("Start shutdown()");
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) sel.shutdown();
    }
    logger.info("Finish shutdown()");
  }
  
  public boolean isAlive() {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) return true;
    }
    return false;
  }
  
  synchronized public void waitForExecutorTermination(long checkPeriod) throws InterruptedException {
    while(isAlive()) {
      wait(checkPeriod);
    }
  }
  
  public List<DataflowTaskExecutor> getExecutors() { return taskExecutors; }
  
  public class DataflowTaskWorkerEventListenter extends NodeEventWatcher {
    public DataflowTaskWorkerEventListenter(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true/*persistent*/);
      watchModify(dflRegistry.getDataflowTasksWorkerEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        DataflowEvent taskEvent = getRegistry().getDataAs(event.getPath(), DataflowEvent.class);
        if(taskEvent == DataflowEvent.PAUSE) {
          if(isAlive()) shutdown() ;
        } else if(taskEvent == DataflowEvent.STOP) {
          if(isAlive()) shutdown() ;
        }
      }
      System.out.println("event = " + event.getType() + ", path = " + event.getPath());
    }
  }
}