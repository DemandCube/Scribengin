package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.RegistryLogger;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
@JmxBean("role=dataflow-worker, type=DataflowTaskExecutorService, dataflowName=DataflowTaskExecutorService")
public class DataflowTaskExecutorService {
  private Logger logger = LoggerFactory.getLogger(DataflowTaskExecutorService.class);

  @Inject
  private DataflowContainer container;
  
  @Inject
  private VMDescriptor vmDescriptor ;

  private RegistryLogger registryLogger ;
  private DataflowWorkerEventListenter dataflowTaskEventListener ;
  private DataflowDescriptor dataflowDescriptor;
  private List<DataflowTaskExecutor> taskExecutors;
  private DataflowWorkerStatus workerStatus = DataflowWorkerStatus.INIT;
  private boolean kill = false ;
  
  @Inject
  public void onInit(DataflowRegistry dflRegistry) throws Exception {
    logger.info("Start onInit()");
    Node workerNode = dflRegistry.getWorkerNode(vmDescriptor.getId()) ;
    registryLogger = new RegistryLogger(dflRegistry.getRegistry(),  workerNode.getPath(), "logs/events");
    dataflowTaskEventListener = new DataflowWorkerEventListenter(dflRegistry);
    dataflowDescriptor = dflRegistry.getDataflowDescriptor();
    
    int numOfExecutors = dataflowDescriptor.getNumberOfExecutorsPerWorker();
    taskExecutors = new ArrayList<DataflowTaskExecutor>();
    for(int i = 0; i < numOfExecutors; i++) {
      DataflowTaskExecutorDescriptor descriptor = new DataflowTaskExecutorDescriptor ("executor-" + i);
      DataflowTaskExecutor executor = new DataflowTaskExecutor(descriptor, container);
      taskExecutors.add(executor);
    }
    logger.info("Finish onInit()");
  }
  
  public void start() throws Exception {
    logger.info("Start start()");
    registryLogger.info("start-start", "DataflowTaskExecutorService: start start()");
    for(int i = 0; i < taskExecutors.size(); i++) {
      DataflowTaskExecutor executor = taskExecutors.get(i);
      executor.start();
    }
    workerStatus = DataflowWorkerStatus.RUNNING;
    container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
    registryLogger.info("finish-start", "DataflowTaskExecutorService: finish start()");
    logger.info("Finish start()");
  }
  
  
  void interrupt() throws Exception {
    System.err.println("  DataflowTaskExecutorService: Interrupt dataflow worker executor");
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) sel.interrupt();
    }
    waitForExecutorTermination(500);
    System.err.println("  DataflowTaskExecutorService: Interrupt dataflow worker executor done!!!!!!!!!!!!!");
  }
  
  public void pause() throws Exception {
    registryLogger.info("start-pause", "DataflowTaskExecutorService: start pause()");
    workerStatus = DataflowWorkerStatus.PAUSING;
    container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
    interrupt();
    workerStatus = DataflowWorkerStatus.PAUSE;
    container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
    registryLogger.info("finish-pause", "DataflowTaskExecutorService: finish pause()");
  }
  
  @PreDestroy
  public void shutdown() throws Exception {
    if(kill) return;
    logger.info("Start shutdown()");
    registryLogger.info("start-shutdown", "DataflowTaskExecutorService: start shutdown()");
    if(workerStatus != DataflowWorkerStatus.TERMINATED) {
      System.err.println("DataflowTaskExecutorService: shutdown()");
      workerStatus = DataflowWorkerStatus.TERMINATING;
      container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
      dataflowTaskEventListener.setComplete();
      interrupt() ;
      workerStatus = DataflowWorkerStatus.TERMINATED;
      container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
      System.err.println("DataflowTaskExecutorService: shutdown() done!");
    }
    System.err.println("DataflowTaskExecutorService: Finish shutdown()");
    registryLogger.info("finish-shutdown", "DataflowTaskExecutorService: finish shutdown()");
    logger.info("Finish shutdown()");
  }
  
  public void simulateKill() throws Exception {
    logger.info("Start kill()");
    registryLogger.info("start-simulate-kill", "DataflowTaskExecutorService: start simulateKill()");
    kill = true ;
    if(workerStatus != DataflowWorkerStatus.TERMINATED) {
      for(DataflowTaskExecutor sel : taskExecutors) {
        if(sel.isAlive()) sel.kill();
      }
    }
    registryLogger.info("finish-simulate-kill", "DataflowTaskExecutorService: finish simulateKill()");
    logger.info("Finish kill()");
  }
  
  public boolean isAlive() {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) {
        return true;
      }
    }
    return false;
  }
  
  synchronized void waitForExecutorTermination(long checkPeriod) throws InterruptedException {
    while(isAlive()) {
      wait(checkPeriod);
    }
  }
  
  synchronized public void waitForTerminated(long checkPeriod) throws InterruptedException, RegistryException {
    while(workerStatus != DataflowWorkerStatus.TERMINATED) {
      waitForExecutorTermination(checkPeriod);
      if(workerStatus == DataflowWorkerStatus.RUNNING) {
        workerStatus = DataflowWorkerStatus.TERMINATED;
        container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
      }
      wait(checkPeriod);
    }
  }
  
  public class DataflowWorkerEventListenter extends NodeEventWatcher {
    public DataflowWorkerEventListenter(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true/*persistent*/);
      watchModify(dflRegistry.getWorkerEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        DataflowEvent taskEvent = getRegistry().getDataAs(event.getPath(), DataflowEvent.class);
        if(taskEvent == DataflowEvent.PAUSE) {
          System.err.println("DataflowTaskExecutorService: Dataflow worker detect pause event!");
          pause() ;
        } else if(taskEvent == DataflowEvent.STOP) {
          System.err.println("DataflowTaskExecutorService: Dataflow worker detect stop event!");
          shutdown() ;
        } else if(taskEvent == DataflowEvent.RESUME) {
          System.err.println("DataflowTaskExecutorService: Dataflow worker detect resume event!");
          start() ;
        }
      }
    }
  }
}